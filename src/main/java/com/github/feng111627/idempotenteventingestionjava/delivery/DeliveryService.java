package com.github.feng111627.idempotenteventingestionjava.delivery;

import com.github.feng111627.idempotenteventingestionjava.config.RetryProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * 带重试与退避的投递服务。
 * <p>
 * 策略：最多 {@code maxAttempts} 次尝试，临时失败按
 * {@code initialBackoffMs * multiplier^(attempt-1)} 指数退避；
 * 重试耗尽后事件进入死信区（DEAD_LETTER），原因标记为 DOWNSTREAM_UNAVAILABLE。
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DownstreamClient downstream;
    private final EventStore store;
    private final RetryProperties retry;

    public DeliveryService(DownstreamClient downstream, EventStore store, RetryProperties retry) {
        this.downstream = downstream;
        this.store = store;
        this.retry = retry;
    }

    public void deliverWithRetry(EventRecord record) {
        for (int attempt = 1; attempt <= retry.getMaxAttempts(); attempt++) {
            record.setAttempts(attempt);
            record.setStatus(attempt == 1 ? EventStatus.PROCESSING : EventStatus.RETRYING);
            record.setUpdatedAt(Instant.now());
            store.update(record);
            try {
                downstream.deliver(record.getEventId(), record.getPayload());
                record.setStatus(EventStatus.DELIVERED);
                record.setUpdatedAt(Instant.now());
                store.update(record);
                log.info("event={} 第 {} 次尝试投递成功", record.getEventId(), attempt);
                return;
            } catch (DownstreamUnavailableException e) {
                log.warn("event={} 第 {} 次尝试失败（临时故障）：{}", record.getEventId(), attempt, e.getMessage());
                if (attempt < retry.getMaxAttempts()) {
                    sleep(backoffMs(attempt));
                }
            }
        }
        record.setStatus(EventStatus.DEAD_LETTER);
        record.setFailureReason(FailureReason.DOWNSTREAM_UNAVAILABLE);
        record.setFailureDetail("重试 " + retry.getMaxAttempts() + " 次后下游仍不可用");
        record.setUpdatedAt(Instant.now());
        store.update(record);
        log.error("event={} 重试耗尽，进入死信区", record.getEventId());
    }

    /** 指数退避：initialBackoffMs * multiplier^(attempt-1)。 */
    public long backoffMs(int attempt) {
        return Math.round(retry.getInitialBackoffMs() * Math.pow(retry.getBackoffMultiplier(), attempt - 1));
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
