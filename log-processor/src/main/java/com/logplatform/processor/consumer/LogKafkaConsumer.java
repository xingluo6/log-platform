package com.logplatform.processor.consumer;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.logplatform.processor.alert.AlertEngine;
import com.logplatform.processor.storage.DuckDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Component
public class LogKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(LogKafkaConsumer.class);
    private static final String LOG_STREAM_CHANNEL  = "log-stream";
    private static final String LOG_STREAM_LEVEL_KEY = "config:logstream:level";

    private final DuckDBService duckDBService;
    private final AlertEngine alertEngine;
    private final StringRedisTemplate redisTemplate;
    private final ExecutorService storageExecutor;
    private final ExecutorService alertExecutor;

    public LogKafkaConsumer(
            DuckDBService duckDBService,
            AlertEngine alertEngine,
            StringRedisTemplate redisTemplate,
            @Qualifier("storageExecutor") ExecutorService storageExecutor,
            @Qualifier("alertExecutor") ExecutorService alertExecutor
    ) {
        this.duckDBService   = duckDBService;
        this.alertEngine     = alertEngine;
        this.redisTemplate   = redisTemplate;
        this.storageExecutor = storageExecutor;
        this.alertExecutor   = alertExecutor;
    }

    @KafkaListener(topics = "raw-logs", groupId = "log-processor-group")
    public void consume(String message) {
        try {
            JSONObject json = JSON.parseObject(message);

            String timestamp  = json.getString("@timestamp");
            String level      = json.getString("level");
            String service    = json.getString("service");
            String msg        = json.getString("message");
            String loggerName = json.getString("logger_name");
            String threadName = json.getString("thread_name");

            if (timestamp == null || level == null) return;

            // 正确解析带时区偏移的时间戳 → UTC Instant 字符串
            // "2026-03-23T09:04:02+08:00" → "2026-03-23T01:04:02Z"
            // DuckDBService.insertLog 会把 UTC 再转为上海时间存储
            if (timestamp.contains("+")) {
                try {
                    timestamp = java.time.OffsetDateTime.parse(timestamp)
                            .toInstant().toString();
                } catch (Exception e) {
                    timestamp = timestamp.substring(0, timestamp.lastIndexOf("+")) + "Z";
                }
            }
            final String finalTs = timestamp;

            // ── 存储线程池：写入 DuckDB ───────────────────
            storageExecutor.submit(() ->
                    duckDBService.insertLog(finalTs, level, service, msg, loggerName, threadName)
            );

            // ── 告警线程池：只对业务日志匹配规则 ─────────
            if (shouldEvaluate(loggerName)) {
                Map<String, String> logData = new HashMap<>();
                logData.put("level",   level);
                logData.put("service", service);
                logData.put("message", msg);
                alertExecutor.submit(() -> alertEngine.evaluate(logData));
            }

            // ── WebSocket 日志流推送 ─────────────────────
            // 每次从 Redis 读取开关状态，切换后立即生效
            String streamLevel = redisTemplate.opsForValue().get(LOG_STREAM_LEVEL_KEY);

            // streamLevel 为 null 时默认全量推送
            // （确保首次启动、Redis 未设置时也能看到日志流）
            boolean isAll         = streamLevel == null || "ALL".equals(streamLevel);
            boolean isWarnOrError = "WARN".equals(level) || "ERROR".equals(level);
            boolean shouldPush    = isAll || isWarnOrError;

            if (shouldPush) {
                JSONObject streamMsg = new JSONObject();
                streamMsg.put("type",    "log");
                streamMsg.put("service", service);
                streamMsg.put("level",   level);
                streamMsg.put("message", msg);
                // 使用当前上海时间格式化，与前端时钟一致
                streamMsg.put("time",
                        LocalTime.now(java.time.ZoneId.of("Asia/Shanghai"))
                                .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                redisTemplate.convertAndSend(LOG_STREAM_CHANNEL, streamMsg.toJSONString());
            }

        } catch (Exception e) {
            log.error("[Processor] parse failed: {}", e.getMessage());
        }
    }

    /**
     * 过滤 Spring/Tomcat/Kafka 等框架日志，只对业务日志做告警匹配
     */
    private boolean shouldEvaluate(String loggerName) {
        if (loggerName == null) return true;
        return !loggerName.startsWith("org.springframework")
                && !loggerName.startsWith("org.apache")
                && !loggerName.startsWith("org.hibernate")
                && !loggerName.startsWith("com.zaxxer")
                && !loggerName.startsWith("io.netty")
                && !loggerName.startsWith("kafka");
    }
}