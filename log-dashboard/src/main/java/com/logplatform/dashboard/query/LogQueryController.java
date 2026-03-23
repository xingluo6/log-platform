package com.logplatform.dashboard.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * log-dashboard 的查询控制器
 * 职责：把前端请求转发给 log-processor（8083），自身不直接操作 DuckDB
 * 调用链：前端 → localhost:8084/api/* → 本类 → localhost:8083/internal/query/*
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class LogQueryController {

    private static final Logger log = LoggerFactory.getLogger(LogQueryController.class);
    private static final String PROCESSOR_URL = "http://localhost:8083/internal/query";

    private final RestTemplate restTemplate = new RestTemplate();

    // ── 数据查询（转发给 processor）────────────────────────

    @GetMapping("/error-rate")
    public Object getErrorRate(@RequestParam(defaultValue = "30") int minutes) {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/error-rate?minutes=" + minutes, List.class);
    }

    @GetMapping("/alerts")
    public Object getAlerts(@RequestParam(defaultValue = "50") int limit) {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/alerts?limit=" + limit, List.class);
    }

    @GetMapping("/service-status")
    public Object getServiceStatus() {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/service-status", List.class);
    }

    @GetMapping("/stats")
    public Object getStats() {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/stats", Map.class);
    }

    @GetMapping("/level-stats")
    public Object getLevelStats(@RequestParam(defaultValue = "30") int minutes) {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/logs/level-stats?minutes=" + minutes, List.class);
    }

    @GetMapping("/log-volume")
    public Object getLogVolume(@RequestParam(defaultValue = "30") int minutes) {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/logs/volume?minutes=" + minutes, List.class);
    }

    /**
     * 查询所有历史服务（服务健康留痕用）
     */
    @GetMapping("/logs/service-history")
    public Object getServiceHistory() {
        return restTemplate.getForObject(
                PROCESSOR_URL + "/logs/service-history", List.class);
    }

    /**
     * 清除数据（转发给 processor 执行实际删除）
     */
    @PostMapping("/cleanup")
    public Object cleanup(@RequestBody Map<String, Object> body) {
        log.info("[Dashboard] forwarding cleanup request: {}", body);
        return restTemplate.postForObject(
                PROCESSOR_URL + "/cleanup", body, Map.class);
    }
}