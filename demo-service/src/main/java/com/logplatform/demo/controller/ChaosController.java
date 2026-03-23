package com.logplatform.demo.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/chaos")
public class ChaosController {

    private static final Logger log = LoggerFactory.getLogger(ChaosController.class);

    // 模拟正常请求
    @GetMapping("/normal")
    public String normal() {
        log.info("normal request processed successfully");
        return "ok";
    }

    // 模拟错误请求
    @GetMapping("/error")
    public String error() {
        log.error("database connection timeout, retrying...");
        return "error";
    }

    // 模拟慢请求
    @GetMapping("/slow")
    public String slow() throws InterruptedException {
        Thread.sleep(800);
        log.warn("slow response detected, cost 800ms");
        return "slow";
    }

    // 模拟随机流量（压测用）
    @GetMapping("/random")
    public String random() {
        double rand = Math.random();
        if (rand < 0.1) {
            log.error("random error occurred, rate=10%");
            return "error";
        } else if (rand < 0.2) {
            log.warn("random warn occurred, rate=10%");
            return "warn";
        } else {
            log.info("random request success");
            return "ok";
        }
    }
}