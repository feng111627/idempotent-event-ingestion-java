package com.github.feng111627.idempotenteventingestionjava.config;

import com.github.feng111627.idempotenteventingestionjava.retry.RetryScheduler;
import com.github.feng111627.idempotenteventingestionjava.service.EventProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 启动后先做崩溃恢复（未完成事件重新排队），再启动重试扫描，保证重启不丢事件。
 */
@Component
public class StartupLifecycle {

    private static final Logger log = LoggerFactory.getLogger(StartupLifecycle.class);

    private final EventProcessingService service;
    private final RetryScheduler retryScheduler;

    public StartupLifecycle(EventProcessingService service, RetryScheduler retryScheduler) {
        this.service = service;
        this.retryScheduler = retryScheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("应用就绪，开始启动恢复与重试调度……");
        service.recoverAfterRestart();
        retryScheduler.start();
    }
}
