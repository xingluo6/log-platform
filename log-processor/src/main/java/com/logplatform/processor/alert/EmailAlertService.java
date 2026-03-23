package com.logplatform.processor.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import jakarta.mail.internet.MimeMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 邮件告警服务 - 方案B：首次触发立即发送 + 冷却期结束后汇总
 *
 * Redis数据结构：
 *   pending:email:{service}        → Hash，key=ruleName，value=触发次数
 *                                    TTL=throttleMinutes（冷却期）
 *   pending:email:first:{service}  → String "1"，标记首次是否已发
 *                                    TTL=throttleMinutes
 */
@Service
public class EmailAlertService {

    private static final Logger log = LoggerFactory.getLogger(EmailAlertService.class);

    private static final String KEY_TO       = "config:email:to";
    private static final String KEY_USERNAME = "config:email:username";
    private static final String KEY_PASSWORD = "config:email:password";
    private static final String KEY_THROTTLE = "config:email:throttle";
    private static final String KEY_ENABLED  = "config:email:enabled";
    private static final String PENDING_PREFIX    = "pending:email:";
    private static final String FIRST_SENT_PREFIX = "pending:email:first:";

    private final StringRedisTemplate redis;

    @Value("${spring.mail.username:}")  private String defaultUsername;
    @Value("${spring.mail.password:}")  private String defaultPassword;
    @Value("${alert.email.to:}")        private String defaultTo;
    @Value("${alert.email.enabled:false}") private boolean defaultEnabled;
    @Value("${alert.email.throttle-minutes:30}") private int defaultThrottle;

    public EmailAlertService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    // ── 配置读取 ────────────────────────────────────────────

    private String getConfig(String key, String fallback) {
        String val = redis.opsForValue().get(key);
        return (val != null && !val.isBlank()) ? val : fallback;
    }

    private String getPassword() {
        String fromRedis = redis.opsForValue().get(KEY_PASSWORD);
        if (fromRedis != null && !fromRedis.isBlank())
            return decodePassword(fromRedis);
        return defaultPassword;
    }

    private boolean isEnabled() {
        String val = redis.opsForValue().get(KEY_ENABLED);
        return val != null ? Boolean.parseBoolean(val) : defaultEnabled;
    }

    private int getThrottleMinutes() {
        String val = redis.opsForValue().get(KEY_THROTTLE);
        try { return val != null ? Integer.parseInt(val) : defaultThrottle; }
        catch (Exception e) { return defaultThrottle; }
    }

    private String decodePassword(String encoded) {
        if (encoded == null || encoded.isBlank()) return "";
        try {
            return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
        } catch (Exception e) { return encoded; }
    }

    // ── 保存配置 ────────────────────────────────────────────

    public void saveConfig(String to, String username, String password, int throttleMinutes) {
        if (to != null && !to.isBlank()) redis.opsForValue().set(KEY_TO, to);
        if (username != null && !username.isBlank()) redis.opsForValue().set(KEY_USERNAME, username);
        if (password != null && !password.isBlank()) {
            String encoded = Base64.getEncoder()
                    .encodeToString(password.getBytes(StandardCharsets.UTF_8));
            redis.opsForValue().set(KEY_PASSWORD, encoded);
        }
        if (throttleMinutes > 0)
            redis.opsForValue().set(KEY_THROTTLE, String.valueOf(throttleMinutes));
        log.info("[Email] config saved: to={}, throttle={}min", to, throttleMinutes);
    }

    public void setEnabled(boolean enabled) {
        redis.opsForValue().set(KEY_ENABLED, String.valueOf(enabled));
    }

    // ── 测试发送 ────────────────────────────────────────────

    public String sendTest() {
        String username = getConfig(KEY_USERNAME, defaultUsername);
        String password = getPassword();
        String to       = getConfig(KEY_TO, defaultTo);

        if (username.isBlank() || password.isBlank()) return "❌ 未配置发件邮箱或授权码";
        if (to.isBlank()) return "❌ 未配置收件邮箱";

        try {
            JavaMailSenderImpl sender = buildSender(username, password);
            MimeMessage mime = sender.createMimeMessage();
            MimeMessageHelper h = new MimeMessageHelper(mime, true, "UTF-8");
            h.setFrom(username);
            h.setTo(to);
            h.setSubject("✅ 智能运维监控平台 - 邮件测试");
            h.setText("""
                <div style="font-family:Arial,sans-serif;padding:20px">
                  <h2 style="color:#238636">✅ 邮件配置测试成功</h2>
                  <p>如果您收到这封邮件，说明邮件告警功能已正确配置。</p>
                  <p style="color:#666;font-size:12px">发送时间：%s</p>
                </div>
            """.formatted(LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))), true);
            sender.send(mime);
            log.info("[Email] test sent to: {}", to);
            return "✅ 测试邮件已发送至 " + to;
        } catch (Exception e) {
            log.error("[Email] test failed: {}", e.getMessage());
            return "❌ 发送失败：" + e.getMessage();
        }
    }

    // ── 方案B：首次触发立即发 + 冷却期结束后汇总 ────────────

    public void sendIfAllowed(String ruleName, String service, String message) {
        if (!isEnabled()) return;

        String to       = getConfig(KEY_TO, defaultTo);
        String username = getConfig(KEY_USERNAME, defaultUsername);
        String password = getPassword();
        if (to.isBlank() || username.isBlank() || password.isBlank()) {
            log.warn("[Email] incomplete config, skip");
            return;
        }

        int throttle = getThrottleMinutes();
        String pendingKey   = PENDING_PREFIX + service;
        String firstSentKey = FIRST_SENT_PREFIX + service;

        // 追加告警到pending列表（Hash结构：ruleName -> count）
        redis.opsForHash().increment(pendingKey, ruleName, 1);
        redis.expire(pendingKey, Duration.ofMinutes(throttle));

        // 检查是否已发过首次通知
        Boolean firstSent = redis.hasKey(firstSentKey);

        if (!Boolean.TRUE.equals(firstSent)) {
            // 首次触发：立即发送
            redis.opsForValue().set(firstSentKey, "1", Duration.ofMinutes(throttle));
            final String fu = username, fp = password, ft = to, fm = message;
            new Thread(() -> sendFirstAlert(fu, fp, ft, service, ruleName, fm, throttle))
                    .start();
        }
        // 冷却期内：只累计，不发送
        // 冷却期结束后TTL过期，下次触发时firstSentKey不存在，会重新发送汇总
    }

    /**
     * 冷却期结束后，发送汇总邮件（由定时任务或下次触发调用）
     * 当pendingKey存在但firstSentKey不存在时说明需要发汇总
     */
    public void checkAndSendSummary(String service) {
        String pendingKey   = PENDING_PREFIX + service;
        String firstSentKey = FIRST_SENT_PREFIX + service;

        if (!Boolean.TRUE.equals(redis.hasKey(pendingKey))) return;
        if (Boolean.TRUE.equals(redis.hasKey(firstSentKey))) return;

        // 读取pending的告警统计
        Map<Object, Object> counts = redis.opsForHash().entries(pendingKey);
        if (counts.isEmpty()) return;

        String to       = getConfig(KEY_TO, defaultTo);
        String username = getConfig(KEY_USERNAME, defaultUsername);
        String password = getPassword();
        if (to.isBlank() || username.isBlank() || password.isBlank()) return;

        int throttle = getThrottleMinutes();
        // 重置冷却期
        redis.opsForValue().set(firstSentKey, "1", Duration.ofMinutes(throttle));
        redis.delete(pendingKey);

        final Map<Object, Object> finalCounts = new HashMap<>(counts);
        final String fu = username, fp = password, ft = to;
        new Thread(() -> sendSummaryAlert(fu, fp, ft, service, finalCounts, throttle))
                .start();
    }

    private void sendFirstAlert(String username, String password, String to,
                                String service, String ruleName,
                                String message, int throttle) {
        try {
            String time = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String html = """
                <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
                  <div style="background:#c0392b;color:white;padding:16px;border-radius:8px 8px 0 0">
                    <h2 style="margin:0">🚨 告警通知</h2>
                  </div>
                  <div style="background:#f8f9fa;padding:20px;border:1px solid #dee2e6">
                    <table style="width:100%%;border-collapse:collapse">
                      <tr><td style="padding:8px;color:#666;width:100px">服务名</td>
                          <td style="padding:8px;font-weight:bold">%s</td></tr>
                      <tr style="background:white">
                          <td style="padding:8px;color:#666">告警规则</td>
                          <td style="padding:8px;color:#c0392b;font-weight:bold">%s</td></tr>
                      <tr><td style="padding:8px;color:#666">触发时间</td>
                          <td style="padding:8px">%s</td></tr>
                      <tr style="background:white">
                          <td style="padding:8px;color:#666">错误信息</td>
                          <td style="padding:8px;color:#e74c3c">%s</td></tr>
                    </table>
                  </div>
                  <div style="background:#fff3cd;padding:12px;border:1px solid #ffc107;
                              border-top:none;border-radius:0 0 8px 8px;font-size:12px;color:#856404">
                    冷却期 %d 分钟内如有更多告警将在冷却结束后汇总发送。
                  </div>
                </div>
            """.formatted(service, ruleName, time, message, throttle);
            doSend(username, password, to,
                    "🚨 [首次告警] " + service + " - " + ruleName, html);
        } catch (Exception e) {
            log.error("[Email] sendFirstAlert failed: {}", e.getMessage());
        }
    }

    private void sendSummaryAlert(String username, String password, String to,
                                  String service, Map<Object, Object> counts, int throttle) {
        try {
            int total = counts.values().stream()
                    .mapToInt(v -> Integer.parseInt(v.toString())).sum();

            StringBuilder rows = new StringBuilder();
            for (Map.Entry<Object, Object> e : counts.entrySet()) {
                rows.append("""
                    <tr><td style="padding:6px 8px;color:#666">%s</td>
                        <td style="padding:6px 8px;color:#c0392b;font-weight:bold">
                            %s 次</td></tr>
                """.formatted(e.getKey(), e.getValue()));
            }

            String html = """
                <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
                  <div style="background:#e67e22;color:white;padding:16px;border-radius:8px 8px 0 0">
                    <h2 style="margin:0">📊 告警汇总</h2>
                  </div>
                  <div style="background:#f8f9fa;padding:20px;border:1px solid #dee2e6">
                    <p style="margin:0 0 12px">
                      服务 <b>%s</b> 在过去 <b>%d 分钟</b>内共触发
                      <b style="color:#c0392b">%d 次</b>告警：
                    </p>
                    <table style="width:100%%;border-collapse:collapse;
                                  border:1px solid #dee2e6">
                      <tr style="background:#e9ecef">
                          <th style="padding:8px;text-align:left">告警规则</th>
                          <th style="padding:8px;text-align:left">触发次数</th></tr>
                      %s
                    </table>
                  </div>
                  <div style="background:#d4edda;padding:12px;border:1px solid #c3e6cb;
                              border-top:none;border-radius:0 0 8px 8px;
                              font-size:12px;color:#155724">
                    冷却期已结束，监控恢复正常。如问题持续将再次通知。
                  </div>
                </div>
            """.formatted(service, throttle, total, rows);

            doSend(username, password, to,
                    "📊 [告警汇总] " + service + " 累计 " + total + " 次", html);
        } catch (Exception e) {
            log.error("[Email] sendSummaryAlert failed: {}", e.getMessage());
        }
    }

    private void doSend(String username, String password,
                        String to, String subject, String html) throws Exception {
        JavaMailSenderImpl sender = buildSender(username, password);
        MimeMessage mime = sender.createMimeMessage();
        MimeMessageHelper h = new MimeMessageHelper(mime, true, "UTF-8");
        h.setFrom(username);
        h.setTo(to);
        h.setSubject(subject);
        h.setText(html, true);
        sender.send(mime);
        log.info("[Email] sent: subject={}, to={}", subject, to);
    }

    private JavaMailSenderImpl buildSender(String username, String password) {
        JavaMailSenderImpl sender = new JavaMailSenderImpl();
        sender.setHost("smtp.qq.com");
        sender.setPort(465);
        sender.setUsername(username);
        sender.setPassword(password);
        sender.setDefaultEncoding("UTF-8");
        Properties props = sender.getJavaMailProperties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.ssl.enable", "true");
        props.put("mail.smtp.timeout", "5000");
        props.put("mail.smtp.connectiontimeout", "5000");
        return sender;
    }
}