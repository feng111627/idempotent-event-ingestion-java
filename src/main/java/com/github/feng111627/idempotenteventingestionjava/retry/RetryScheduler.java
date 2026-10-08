package com.github.feng111627.idempotenteventingestionjava.retry;

import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.service.EventProcessingService;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 进程内重试调度器：固定周期扫描到期 RETRYING 事件并重新排队。纯本地实现，无外部依赖。
 * 扫描本身只做“到期判断 + 入队”，实际投递仍在业务工作线程，扫描线程不会被慢投递阻塞。
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final EventProcessingService service;
    private final AppProperties properties;
    private ScheduledExecutorService executor;

    public RetryScheduler(EventProcessingService service, AppProperties properties) {
        this.service = service;
        this.properties = properties;
    }

    /** 启动周期扫描（由启动监听器调用）。 */
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "retry-scanner");
            t.setDaemon(true);
            return t;
        });
        long interval = properties.scanIntervalMillis();
        executor.scheduleWithFixedDelay(this::scanSafely, interval, interval, TimeUnit.MILLISECONDS);
        log.info("重试扫描调度器已启动，扫描周期={}ms", interval);
    }

    private void scanSafely() {
        try {
            int queued = service.dispatchDueRetries(System.currentTimeMillis());
            if (queued > 0) {
                log.info("重试扫描本次排队到期事件数={}", queued);
            }
        } catch (RuntimeException ex) {
            // 扫描异常不能打死调度线程
            log.error("重试扫描发生异常，将在下个周期继续", ex);
        }
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
            log.info("重试扫描调度器已停止");
        }
    }
}
