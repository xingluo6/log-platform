package com.logplatform.processor.query;

import com.logplatform.processor.alert.EmailAlertService;
import com.logplatform.processor.storage.DuckDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/internal/query")
@CrossOrigin(origins = "*")
public class LogQueryController {

    private static final Logger log = LoggerFactory.getLogger(LogQueryController.class);

    private static final String KEY_ENABLED        = "config:email:enabled";
    private static final String DISMISS_PREFIX     = "dismissed:alert:";
    private static final String DISMISS_SVC_PREFIX = "dismissed:service:";
    private static final String LOG_STREAM_KEY     = "config:logstream:level";

    private final DuckDBService duckDBService;
    private final StringRedisTemplate redis;
    private final EmailAlertService emailAlertService;

    @Value("${alert.email.enabled:false}")
    private boolean defaultEmailEnabled;

    public LogQueryController(DuckDBService duckDBService,
                              StringRedisTemplate redis,
                              EmailAlertService emailAlertService) {
        this.duckDBService     = duckDBService;
        this.redis             = redis;
        this.emailAlertService = emailAlertService;
    }

    // ── 数据查询 ────────────────────────────────────────────

    @GetMapping("/error-rate")
    public List<Map<String, Object>> getErrorRate(
            @RequestParam(defaultValue = "30") int minutes) {
        return duckDBService.queryErrorRate(minutes);
    }

    @GetMapping("/alerts")
    public List<Map<String, Object>> getAlerts(
            @RequestParam(defaultValue = "50") int limit) {
        return duckDBService.queryAlerts(limit);
    }

    @GetMapping("/service-status")
    public List<Map<String, Object>> getServiceStatus() {
        List<Map<String, Object>> all = duckDBService.queryServiceStatus();
        Set<String> dismissedSvcs = getDismissedServices();
        return all.stream()
                .filter(s -> !dismissedSvcs.contains(s.get("service")))
                .collect(Collectors.toList());
    }

    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        return duckDBService.queryStats();
    }

    @GetMapping("/logs/recent")
    public List<Map<String, Object>> getRecentLogs(
            @RequestParam String service,
            @RequestParam(defaultValue = "100") int limit) {
        return duckDBService.queryRecentLogs(service, limit);
    }

    @GetMapping("/logs/services")
    public List<Map<String, Object>> getAllServices() {
        return duckDBService.queryAllServices();
    }

    @GetMapping("/logs/level-stats")
    public List<Map<String, Object>> getLevelStats(
            @RequestParam(defaultValue = "30") int minutes) {
        return duckDBService.queryLogLevelStats(minutes);
    }

    @GetMapping("/logs/volume")
    public List<Map<String, Object>> getLogVolume(
            @RequestParam(defaultValue = "30") int minutes) {
        return duckDBService.queryLogVolume(minutes);
    }

    /**
     * 查询所有历史服务及最后活跃时间（服务健康留痕用）
     */
    @GetMapping("/logs/service-history")
    public List<Map<String, Object>> getServiceHistory() {
        return duckDBService.queryServiceHistory();
    }

    /**
     * 清除数据接口
     * range: 1h / 1d / 1w / 4w / all
     */
    @PostMapping("/cleanup")
    public Map<String, Object> cleanup(@RequestBody Map<String, Object> body) {
        String range = (String) body.getOrDefault("range", "1d");
        log.info("[Cleanup] executing range={}", range);
        return duckDBService.cleanupByRange(range);
    }

    // ── 邮件配置 ────────────────────────────────────────────

    @GetMapping("/config/email-status")
    public Map<String, Object> getEmailStatus() {
        String val = redis.opsForValue().get(KEY_ENABLED);
        boolean enabled = val != null ? Boolean.parseBoolean(val) : defaultEmailEnabled;
        String to       = redis.opsForValue().get("config:email:to");
        String username = redis.opsForValue().get("config:email:username");
        String throttle = redis.opsForValue().get("config:email:throttle");
        String password = redis.opsForValue().get("config:email:password");
        return Map.of(
                "enabled",            enabled,
                "to",                 to != null ? to : "",
                "username",           username != null ? username : "",
                "throttleMinutes",    throttle != null ? Integer.parseInt(throttle) : 30,
                "passwordConfigured", password != null && !password.isBlank()
        );
    }

    @PostMapping("/config/email-toggle")
    public Map<String, Object> toggleEmail(@RequestBody Map<String, Object> body) {
        boolean enabled = Boolean.parseBoolean(body.getOrDefault("enabled", false).toString());
        emailAlertService.setEnabled(enabled);
        return Map.of("enabled", enabled);
    }

    @PostMapping("/config/email-save")
    public Map<String, Object> saveEmailConfig(@RequestBody Map<String, Object> body) {
        String to       = (String) body.get("to");
        String username = (String) body.get("username");
        String password = (String) body.get("password");
        int throttle    = body.containsKey("throttleMinutes")
                ? Integer.parseInt(body.get("throttleMinutes").toString()) : 30;
        emailAlertService.saveConfig(to, username, password, throttle);
        return Map.of("success", true, "msg", "配置已保存");
    }

    @PostMapping("/config/email-test")
    public Map<String, Object> testEmail() {
        String result = emailAlertService.sendTest();
        return Map.of("success", result.startsWith("✅"), "message", result);
    }

    // ── 日志流开关 ──────────────────────────────────────────

    @PostMapping("/config/logstream-level")
    public Map<String, Object> setLogStreamLevel(@RequestBody Map<String, Object> body) {
        String level = (String) body.getOrDefault("level", "WARN_ERROR");
        redis.opsForValue().set(LOG_STREAM_KEY, level);
        return Map.of("level", level);
    }

    @GetMapping("/config/logstream-level")
    public Map<String, Object> getLogStreamLevel() {
        String level = redis.opsForValue().get(LOG_STREAM_KEY);
        return Map.of("level", level != null ? level : "WARN_ERROR");
    }

    // ── 告警消除 ────────────────────────────────────────────

    @PostMapping("/alert/dismiss")
    public Map<String, Object> dismissAlert(@RequestBody Map<String, Object> body) {
        String key = (String) body.get("key");
        if (key == null || key.isBlank())
            return Map.of("success", false, "msg", "key不能为空");

        // 消除告警分组
        // 存消除时间戳，前端用来判断是否有新告警出现
        redis.opsForValue().set(DISMISS_PREFIX + key,
                java.time.LocalDateTime.now().toString(), Duration.ofHours(24));

        // 同时消除对应服务的健康状态显示
        String service = key.contains("||") ? key.split("\\|\\|")[1] : key;
        redis.opsForValue().set(DISMISS_SVC_PREFIX + service, "1", Duration.ofHours(24));

        String dismissedAt = java.time.LocalDateTime.now().toString();
        log.info("[Alert] dismissed: key={}, service={}", key, service);
        return Map.of("success", true, "dismissedAt", dismissedAt);
    }

    @GetMapping("/alert/dismissed")
    public Map<String, Object> getDismissed() {
        Set<String> keys = redis.keys(DISMISS_PREFIX + "*");
        if (keys == null) return Map.of("dismissed", Map.of());
        // 返回 { key -> dismissedAt 时间戳 } 的 Map
        // 前端用来判断：如果新告警时间 > dismissedAt，重新显示
        Map<String, String> dismissed = new java.util.HashMap<>();
        for (String k : keys) {
            String alertKey = k.replace(DISMISS_PREFIX, "");
            String ts = redis.opsForValue().get(k);
            dismissed.put(alertKey, ts != null ? ts : "");
        }
        return Map.of("dismissed", dismissed);
    }

    // ── 私有工具方法 ────────────────────────────────────────

    /**
     * 恢复单个已消除的告警分组
     */
    @PostMapping("/alert/dismiss-restore")
    public Map<String, Object> restoreDismissed(@RequestBody Map<String, Object> body) {
        String key = (String) body.get("key");
        if (key == null || key.isBlank())
            return Map.of("success", false);
        redis.delete(DISMISS_PREFIX + key);
        // 同时恢复对应服务的健康状态
        String service = key.contains("||") ? key.split("\\|\\|")[1] : key;
        redis.delete(DISMISS_SVC_PREFIX + service);
        log.info("[Alert] restored: {}", key);
        return Map.of("success", true);
    }

    private Set<String> getDismissedServices() {
        Set<String> keys = redis.keys(DISMISS_SVC_PREFIX + "*");
        if (keys == null) return Set.of();
        return keys.stream()
                .map(k -> k.replace(DISMISS_SVC_PREFIX, ""))
                .collect(Collectors.toSet());
    }
}