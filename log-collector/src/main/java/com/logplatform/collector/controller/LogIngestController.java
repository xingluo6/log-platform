package com.logplatform.collector.controller;

import com.alibaba.fastjson2.JSONObject;
import com.logplatform.collector.server.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 通用HTTP日志接入接口
 *
 * Python端全量透传，平台侧统一做智能处理：
 *   1. HTTP状态码识别（Uvicorn access日志）
 *      - 5xx → ERROR，触发告警
 *      - 4xx → WARN
 *      - 2xx/3xx → INFO，存储但不告警
 *   2. 异常关键词识别
 *      - Traceback / Exception / Error → ERROR
 *   3. 警告关键词识别
 *      - timeout / retry / 超时 → WARN
 *   4. 特殊格式处理
 *      - stderr输出（level=ERROR传入）→ 保持ERROR
 *      - print输出（level=INFO传入）→ 按内容重新判断
 *
 * Python接入（项目入口加两行）：
 *   from platform_handler import install
 *   install("shuyan-ai")
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class LogIngestController {

    private static final Logger log = LoggerFactory.getLogger(LogIngestController.class);

    // Uvicorn access日志格式：匹配HTTP状态码
    // 示例：127.0.0.1:56123 - "GET /api/xxx HTTP/1.1" 200 OK
    private static final Pattern HTTP_STATUS_PATTERN =
            Pattern.compile("\"[A-Z]+\\s+\\S+\\s+HTTP/\\d\\.\\d\"\\s+(\\d{3})");

    // ERROR级别关键词（不区分大小写）
    private static final String[] ERROR_KEYWORDS = {
            "traceback", "exception", "critical", "fatal",
            "failed", "failure", "crash",
            "连接失败", "连接断开", "崩溃", "致命错误"
    };

    // WARN级别关键词
    private static final String[] WARN_KEYWORDS = {
            "warning", "warn", "timeout", "timed out",
            "retry", "retrying", "retried",
            "slow", "deprecated", "refused",
            "超时", "重试", "警告", "慢响应", "拒绝连接"
    };

    private final KafkaProducerService kafkaProducerService;

    public LogIngestController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @PostMapping("/log")
    public String ingest(@RequestBody JSONObject body) {
        try {
            String service    = body.getString("service");
            String level      = body.getString("level");
            String message    = body.getString("message");
            String loggerName = body.getString("logger_name");
            String threadName = body.getString("thread_name");

            if (service == null || message == null) {
                return "error: service and message are required";
            }

            if (level == null) level = "INFO";

            // 平台侧统一做智能level解析
            String resolvedLevel = resolveLevel(level, loggerName, message);

            // 构造与Logback TCP格式一致的JSON
            JSONObject payload = new JSONObject();
            payload.put("@timestamp", Instant.now().toString());
            payload.put("@version", "1");
            payload.put("service",     service);
            payload.put("level",       resolvedLevel);
            payload.put("message",     message);
            payload.put("logger_name", loggerName != null ? loggerName : "app." + service);
            payload.put("thread_name", threadName != null ? threadName : "http");

            kafkaProducerService.send(payload.toJSONString());

            log.debug("[Ingest] service={}, level={}→{}", service, level, resolvedLevel);
            return "ok";

        } catch (Exception e) {
            log.error("[Ingest] failed: {}", e.getMessage());
            return "error: " + e.getMessage();
        }
    }

    /**
     * 智能判断日志真实级别
     *
     * 优先级：
     *   1. 已是ERROR → 直接返回
     *   2. Uvicorn HTTP access日志 → 按状态码
     *   3. 已是WARN → 直接返回
     *   4. 消息内容关键词匹配
     *   5. 原始level兜底
     */
    private String resolveLevel(String originalLevel, String loggerName, String message) {
        String upper = originalLevel.toUpperCase();

        // 1. 已经是 ERROR，不降级
        if ("ERROR".equals(upper)) return "ERROR";

        // 2. Uvicorn / HTTP access 日志：按状态码重新判断
        boolean isHttpLog = loggerName != null && (
                loggerName.contains("uvicorn") ||
                        loggerName.contains("access")  ||
                        loggerName.equals("stdout")    // print输出的HTTP日志
        );

        // 消息里有HTTP状态码格式就识别（不限logger名，兜底）
        Matcher m = HTTP_STATUS_PATTERN.matcher(message);
        if (m.find()) {
            int status = Integer.parseInt(m.group(1));
            if (status >= 500) return "ERROR";
            if (status >= 400) return "WARN";
            return "INFO";  // 2xx/3xx
        }

        // 3. 已经是 WARN
        if ("WARN".equals(upper) || "WARNING".equals(upper)) return "WARN";

        // 4. 消息内容关键词匹配
        String msgLower = message.toLowerCase();

        for (String kw : ERROR_KEYWORDS) {
            if (msgLower.contains(kw)) return "ERROR";
        }

        for (String kw : WARN_KEYWORDS) {
            if (msgLower.contains(kw)) return "WARN";
        }

        // 5. 原始level兜底
        return "INFO".equals(upper) ? "INFO" : "INFO";
    }
}