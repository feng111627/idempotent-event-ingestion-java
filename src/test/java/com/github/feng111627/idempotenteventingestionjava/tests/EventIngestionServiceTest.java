package com.github.feng111627.idempotenteventingestionjava.tests;

import com.github.feng111627.idempotenteventingestionjava.api.IngestRequest;
import com.github.feng111627.idempotenteventingestionjava.api.IngestResponse;
import com.github.feng111627.idempotenteventingestionjava.config.RetryProperties;
import com.github.feng111627.idempotenteventingestionjava.delivery.DeliveryService;
import com.github.feng111627.idempotenteventingestionjava.delivery.SimulatedDownstreamClient;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.domain.IngestResult;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventIngestionService;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventStore;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("事件接入：重复投递去重、并发投递与校验失败")
@ExtendWith(ScenarioLoggerExtension.class)
class EventIngestionServiceTest {

    @TempDir
    Path tempDir;

    private EventStore store;
    private SimulatedDownstreamClient downstream;
    private EventIngestionService service;

    @BeforeEach
    void setUp() {
        store = new EventStore(tempDir.resolve("journal.jsonl").toString());
        downstream = new SimulatedDownstreamClient();
        RetryProperties retry = new RetryProperties();
        retry.setMaxAttempts(3);
        retry.setInitialBackoffMs(1);
        service = new EventIngestionService(store, new DeliveryService(downstream, store, retry));
    }

    @Test
    @DisplayName("重复投递：第二次返回 DUPLICATE，下游只落地一次")
    void duplicateDeliveryIsDetected() {
        IngestResponse first = service.ingest(new IngestRequest("e1", "数据"));
        IngestResponse second = service.ingest(new IngestRequest("e1", "数据"));

        assertThat(first.result()).isEqualTo(IngestResult.ACCEPTED);
        assertThat(first.status()).isEqualTo(EventStatus.DELIVERED);
        assertThat(second.result()).isEqualTo(IngestResult.DUPLICATE);
        assertThat(second.status()).isEqualTo(EventStatus.DELIVERED);
        assertThat(downstream.deliveredCount("e1")).isEqualTo(1);
        assertThat(store.find("e1").orElseThrow().getDuplicateAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("并发投递：N 个线程同时投递同一事件，只有一个被受理，其余均为 DUPLICATE，下游只落地一次")
    void concurrentDuplicateDelivery() throws Exception {
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<IngestResponse>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                return service.ingest(new IngestRequest("race", "并发数据"));
            }));
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        go.countDown();

        List<IngestResponse> responses = new ArrayList<>();
        for (Future<IngestResponse> f : futures) {
            responses.add(f.get(30, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertThat(responses.stream().filter(r -> r.result() == IngestResult.ACCEPTED)).hasSize(1);
        assertThat(responses.stream().filter(r -> r.result() == IngestResult.DUPLICATE)).hasSize(threads - 1);
        assertThat(downstream.deliveredCount("race")).isEqualTo(1);
        assertThat(store.find("race").orElseThrow().getDuplicateAttempts()).isEqualTo(threads - 1);
    }

    @Test
    @DisplayName("校验失败：返回 REJECTED 并落入失败区，原因 VALIDATION_FAILED")
    void validationFailureIsRejectedAndRecorded() {
        IngestResponse response = service.ingest(new IngestRequest("bad id!", "数据"));
        assertThat(response.result()).isEqualTo(IngestResult.REJECTED);

        var saved = store.find("bad id!").orElseThrow();
        assertThat(saved.getStatus()).isEqualTo(EventStatus.DEAD_LETTER);
        assertThat(saved.getFailureReason()).isEqualTo(FailureReason.VALIDATION_FAILED);
        assertThat(downstream.totalDelivered()).isZero();
    }

    @Test
    @DisplayName("缺少 eventId：抛出校验异常（由接口层映射为 400，不透出底层异常）")
    void missingEventIdRejected() {
        assertThatThrownBy(() -> service.ingest(new IngestRequest(null, "数据")))
                .isInstanceOf(EventValidationException.class);
        assertThatThrownBy(() -> service.ingest(new IngestRequest("  ", "数据")))
                .isInstanceOf(EventValidationException.class);
    }

    @Test
    @DisplayName("下游持续失败：响应仍为 ACCEPTED，事件进入死信区可供查询")
    void downstreamFailureEndsInDeadLetter() {
        IngestResponse response = service.ingest(new IngestRequest("d1", "PERMANENT_FAIL 数据"));
        assertThat(response.result()).isEqualTo(IngestResult.ACCEPTED);
        assertThat(response.status()).isEqualTo(EventStatus.DEAD_LETTER);

        var saved = store.find("d1").orElseThrow();
        assertThat(saved.getFailureReason()).isEqualTo(FailureReason.DOWNSTREAM_UNAVAILABLE);
        // 死信事件的重复投递仍被识别为 DUPLICATE，不会重新写入下游
        IngestResponse dup = service.ingest(new IngestRequest("d1", "PERMANENT_FAIL 数据"));
        assertThat(dup.result()).isEqualTo(IngestResult.DUPLICATE);
        assertThat(dup.status()).isEqualTo(EventStatus.DEAD_LETTER);
    }
}
