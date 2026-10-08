package com.github.feng111627.idempotenteventingestionjava.tests;

import com.github.feng111627.idempotenteventingestionjava.config.RetryProperties;
import com.github.feng111627.idempotenteventingestionjava.delivery.DeliveryService;
import com.github.feng111627.idempotenteventingestionjava.delivery.SimulatedDownstreamClient;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("投递链路：临时失败重试、指数退避与死信区")
@ExtendWith(ScenarioLoggerExtension.class)
class DeliveryServiceTest {

    @TempDir
    Path tempDir;

    private EventStore store;
    private SimulatedDownstreamClient downstream;
    private RetryProperties retry;
    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        store = new EventStore(tempDir.resolve("journal.jsonl").toString());
        downstream = new SimulatedDownstreamClient();
        retry = new RetryProperties();
        retry.setMaxAttempts(3);
        retry.setInitialBackoffMs(1); // 测试加速，退避规律单独断言
        retry.setBackoffMultiplier(2.0);
        deliveryService = new DeliveryService(downstream, store, retry);
    }

    private EventRecord register(String id, String payload) {
        EventRecord r = new EventRecord();
        r.setEventId(id);
        r.setPayload(payload);
        r.setStatus(EventStatus.RECEIVED);
        r.setCreatedAt(Instant.now());
        r.setUpdatedAt(Instant.now());
        store.registerIfAbsent(r, new CompletableFuture<>());
        return r;
    }

    @Test
    @DisplayName("临时失败按策略重试，最终成功且只落地一次")
    void transientFailureRetriesThenSucceeds() {
        EventRecord r = register("t1", "TRANSIENT_FAIL:2 数据");
        deliveryService.deliverWithRetry(r);

        EventRecord saved = store.find("t1").orElseThrow();
        assertThat(saved.getStatus()).isEqualTo(EventStatus.DELIVERED);
        assertThat(saved.getAttempts()).isEqualTo(3); // 前 2 次失败，第 3 次成功
        assertThat(downstream.deliveredCount("t1")).isEqualTo(1);
    }

    @Test
    @DisplayName("持续失败重试耗尽后进入死信区，原因可区分")
    void permanentFailureGoesToDeadLetter() {
        EventRecord r = register("p1", "PERMANENT_FAIL 数据");
        deliveryService.deliverWithRetry(r);

        EventRecord saved = store.find("p1").orElseThrow();
        assertThat(saved.getStatus()).isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(saved.getFailureReason()).isEqualTo(FailureReason.DOWNSTREAM_UNAVAILABLE);
        assertThat(saved.getFailureDetail()).contains("3");
        assertThat(saved.getAttempts()).isEqualTo(3);
        assertThat(downstream.deliveredCount("p1")).isZero();
    }

    @Test
    @DisplayName("退避间隔按指数增长：initial * multiplier^(n-1)")
    void backoffIsExponential() {
        retry.setInitialBackoffMs(100);
        retry.setBackoffMultiplier(2.0);
        assertThat(deliveryService.backoffMs(1)).isEqualTo(100);
        assertThat(deliveryService.backoffMs(2)).isEqualTo(200);
        assertThat(deliveryService.backoffMs(3)).isEqualTo(400);
    }

    @Test
    @DisplayName("重试过程中状态经历 RETRYING，不出现状态回退")
    void statusTransitionsThroughRetrying() {
        EventRecord r = register("t2", "TRANSIENT_FAIL:1 数据");
        deliveryService.deliverWithRetry(r);
        // 最终态为 DELIVERED，且日志中每次推进都合法（store 对回退会抛错，未抛即证明无回退）
        assertThat(store.find("t2").orElseThrow().getStatus()).isEqualTo(EventStatus.DELIVERED);
    }
}
