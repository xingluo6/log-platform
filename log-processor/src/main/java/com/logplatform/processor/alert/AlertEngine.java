package com.logplatform.processor.alert;

import com.alibaba.fastjson2.JSONObject;
import com.logplatform.processor.storage.DuckDBService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
@EnableScheduling
public class AlertEngine {

    private static final Logger log = LoggerFactory.getLogger(AlertEngine.class);
    private static final String ALERT_CHANNEL = "alerts";

    private final DuckDBService duckDBService;
    private final RedisWindowService redisWindowService;
    private final StringRedisTemplate redisTemplate;
    private final EmailAlertService emailAlertService;
    private final List<AlertRule> rules = new ArrayList<>();

    public AlertEngine(DuckDBService duckDBService,
                       RedisWindowService redisWindowService,
                       StringRedisTemplate redisTemplate,
                       EmailAlertService emailAlertService) {
        this.duckDBService    = duckDBService;
        this.redisWindowService = redisWindowService;
        this.redisTemplate    = redisTemplate;
        this.emailAlertService = emailAlertService;
    }

    @PostConstruct
    public void initRules() {
        rules.add(new AlertRule("ERROR_IMMEDIATE", "ERROR"));
        rules.add(new AlertRule("ERROR_RATE_5MIN", 3, 5));
        log.info("[AlertEngine] {} rules loaded", rules.size());
    }

    public void evaluate(Map<String, String> logData) {
        String level   = logData.getOrDefault("level", "");
        String service = logData.getOrDefault("service", "unknown");
        String message = logData.getOrDefault("message", "");

        for (AlertRule rule : rules) {
            boolean matched = switch (rule.getRuleType()) {
                case LEVEL_MATCH -> rule.getTargetLevel().equalsIgnoreCase(level);
                case ERROR_RATE  -> duckDBService.countRecentErrors(
                        service, rule.getWindowMinutes()) >= rule.getThreshold();
            };

            if (matched && redisWindowService.allowAlert(rule.getRuleName(), service)) {
                triggerAlert(rule, service, message);
            }
        }
    }

    private void triggerAlert(AlertRule rule, String service, String message) {
        log.warn("🚨 [ALERT] rule={}, service={}, msg={}",
                rule.getRuleName(), service, message);

        duckDBService.insertAlert(rule.getRuleName(), service, message);

        JSONObject payload = new JSONObject();
        payload.put("type", "alert");
        payload.put("ruleName", rule.getRuleName());
        payload.put("service", service);
        payload.put("message", message);
        payload.put("time", LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
        payload.put("level", "ERROR");
        redisTemplate.convertAndSend(ALERT_CHANNEL, payload.toJSONString());

        // 方案B邮件聚合
        emailAlertService.sendIfAllowed(rule.getRuleName(), service, message);
    }

    /**
     * 定时扫描冷却期结束的服务，发送汇总邮件
     * 每2分钟执行一次
     */
    @Scheduled(fixedDelay = 120000)
    public void scheduledSummaryCheck() {
        try {
            Set<String> keys = redisTemplate.keys("pending:email:*");
            if (keys == null) return;
            for (String key : keys) {
                if (key.startsWith("pending:email:first:")) continue;
                String service = key.replace("pending:email:", "");
                emailAlertService.checkAndSendSummary(service);
            }
        } catch (Exception e) {
            log.error("[AlertEngine] summaryCheck failed: {}", e.getMessage());
        }
    }
}