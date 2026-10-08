package com.github.feng111627.idempotenteventingestionjava.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地异步处理线程池：事件接收后立即返回 202，投递在工作线程执行。
 */
@Configuration
public class AsyncConfig {

    @Bean(name = "eventWorker", destroyMethod = "shutdown")
    public ExecutorService eventWorker() {
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "event-worker-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        return Executors.newFixedThreadPool(4, factory);
    }
}
