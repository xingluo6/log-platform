package com.logplatform.processor.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * 基于Redis ZSet的滑动窗口限流
 * 防止同一规则在短时间内重复触发告警（告警风暴抑制）
 */
@Service
public class RedisWindowService {

    private static final Logger log = LoggerFactory.getLogger(RedisWindowService.class);

    // 同一规则N秒内最多触发M次告警
    private static final int WINDOW_SECONDS = 60;
    private static final int MAX_ALERTS_PER_WINDOW = 3;

    private final StringRedisTemplate redisTemplate;

    public RedisWindowService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 判断是否允许触发告警
     * 返回true：允许触发
     * 返回false：被限流，抑制本次告警
     */
    public boolean allowAlert(String ruleName, String service) {
        String key = "alert:window:" + ruleName + ":" + service;
        long now = Instant.now().toEpochMilli();
        long windowStart = now - (WINDOW_SECONDS * 1000L);

        // 删除窗口外的旧记录
        redisTemplate.opsForZSet().removeRangeByScore(key, 0, windowStart);

        // 查当前窗口内的告警次数
        Long count = redisTemplate.opsForZSet().size(key);

        if (count != null && count >= MAX_ALERTS_PER_WINDOW) {
            log.warn("[RateLimit] alert suppressed: rule={}, service={}, count={}/{}",
                    ruleName, service, count, MAX_ALERTS_PER_WINDOW);
            return false;
        }

        // 记录本次触发
        redisTemplate.opsForZSet().add(key, String.valueOf(now), now);
        // 设置key过期时间，避免内存泄漏
        redisTemplate.expire(key, java.time.Duration.ofSeconds(WINDOW_SECONDS * 2));

        return true;
    }
}