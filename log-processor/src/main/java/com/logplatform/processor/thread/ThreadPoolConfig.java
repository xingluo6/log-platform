package com.logplatform.processor.thread;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 三个独立线程池，防止慢任务拖垮整体链路：
 *   parseExecutor  - 日志解析（CPU密集）
 *   storageExecutor - DuckDB写入（IO密集）
 *   alertExecutor  - 告警规则匹配+推送
 */
@Configuration
public class ThreadPoolConfig {

    @Bean(name = "parseExecutor")
    public ExecutorService parseExecutor() {
        return new ThreadPoolExecutor(
                2, 4,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> new Thread(r, "parse-" + r.hashCode()),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Bean(name = "storageExecutor")
    public ExecutorService storageExecutor() {
        return new ThreadPoolExecutor(
                2, 4,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(2000),
                r -> new Thread(r, "storage-" + r.hashCode()),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    @Bean(name = "alertExecutor")
    public ExecutorService alertExecutor() {
        return new ThreadPoolExecutor(
                1, 2,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(500),
                r -> new Thread(r, "alert-" + r.hashCode()),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}