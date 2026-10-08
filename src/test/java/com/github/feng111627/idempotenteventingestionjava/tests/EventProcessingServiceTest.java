package com.github.feng111627.idempotenteventingestionjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.downstream.LandingLog;
import com.github.feng111627.idempotenteventingestionjava.downstream.SimulatedDownstreamClient;
import com.github.feng111627.idempotenteventingestionjava.retry.RetryPolicy;
import com.github.feng111627.idempotenteventingestionjava.service.EventProcessingService;
import com.github.feng111627.idempotenteventingestionjava.store.EventStore;
import com.github.feng111627.idempotenteventingestionjava.web.ApiException;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventRequest;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventResponse;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventStatusView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：事件接收去重、退避重试、失败区、状态一致性（不回退/不重复落地/不丢事件）。
 */
@DisplayName("业务四：事件处理核心服务（去重/重试/失败区/状态机）")
class EventProcessingServiceTest {

    private ObjectMapper objectMapper;
    private AppProperties props;
    private EventStore store;
    private LandingLog landingLog;
    private SimulatedDownstreamClient downstream;
    private EventProcessingService service;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        objectMapper = new ObjectMapper();
        props = new AppProperties(
                tempDir.resolve("events").toString(),
                tempDir.resolve("downstream").toString(),
                3, 100, 1_000, false, 50);
        store = new EventStore(props, objectMapper);
        store.init();
        landingLog = new LandingLog(props);
        landingLog.init();
        downstream = new SimulatedDownstreamClient(objectMapper, landingLog);
        RetryPolicy retryPolicy = new RetryPolicy(props);
        // 同步执行：接收即处理完成，断言确定
        service = new EventProcessingService(store, downstream, retryPolicy, props,
                objectMapper, new DirectExecutorService());
    }

    private EventRequest request(String id, String type, String failMode) {
        ObjectNode payload = objectMapper.createObjectNode().put("type", type);
        if (failMode != null) {
            payload.put("_failMode", failMode);
        }
        return new EventRequest(id, payload);
    }

    @Test
    @DisplayName("用例1：合法事件首次接收返回202/非重复，处理后 SUCCESS，下游恰好落地一次")
    void firstDeliveryAcceptedThenSucceeds() {
        EventResponse resp = service.receive(request("evt-ok", "order.created", null));
        assertFalse(resp.duplicate());
        assertEquals(202, resp.httpStatusHint());

        EventStatusView view = service.query("evt-ok");
        assertEquals("SUCCESS", view.status());
        assertEquals(1, view.attempts());
        assertFalse(view.inFailureZone());
        assertNull(view.failureReason());
        assertNotNull(view.completedAt());
        assertTrue(landingLog.hasLanded("evt-ok"));
    }

    @Test
    @DisplayName("用例2：成功后重复投递返回200/duplicate=true，不重复落地、不重复计数 attempts")
    void duplicateAfterSuccessIsDistinguishableAndNotReDelivered() {
        service.receive(request("evt-dup", "order.created", null));

        EventResponse duplicate1 = service.receive(request("evt-dup", "order.created", null));
        EventResponse duplicate2 = service.receive(request("evt-dup", "order.created", null));

        assertTrue(duplicate1.duplicate());
        assertEquals(200, duplicate1.httpStatusHint());
        assertEquals("SUCCESS", duplicate1.status());

        EventStatusView view = service.query("evt-dup");
        assertEquals("SUCCESS", view.status());
        assertEquals(1, view.attempts(), "重复投递不得增加下游调用次数");
        assertEquals(2, view.duplicateCount(), "两次重复投递必须都被计数");
    }

    @Test
    @DisplayName("用例3：校验失败不重试，直接进入失败区且原因为 VALIDATION_FAILED")
    void validationFailureGoesStraightToFailureZone() {
        // payload 缺 type：在接入层构造合法 JSON 对象，但下游校验失败
        EventRequest bad = new EventRequest("evt-invalid",
                objectMapper.createObjectNode().put("notType", "x"));
        service.receive(bad);

        EventStatusView view = service.query("evt-invalid");
        assertEquals("FAILED", view.status());
        assertTrue(view.inFailureZone());
        assertEquals("VALIDATION_FAILED", view.failureReason());
        assertEquals(1, view.attempts(), "校验失败只尝试一次，不重试");
        assertFalse(landingLog.hasLanded("evt-invalid"));
        assertEquals(1, service.queryFailed().size());
    }

    @Test
    @DisplayName("用例4：eventId 缺失返回 400 业务错误，不写事件、不透出底层异常")
    void missingEventIdRejectedAtDoor() {
        EventRequest noId = new EventRequest(null, objectMapper.createObjectNode().put("type", "x"));
        ApiException ex = assertThrows(ApiException.class, () -> service.receive(noId));
        assertEquals(400, ex.getHttpStatus());
        assertEquals("EVENT_ID_REQUIRED", ex.getCode());
        assertTrue(service.queryAll().isEmpty());
    }

    @Test
    @DisplayName("用例5：临时失败后处于 RETRYING 且带下次重试时间；手动驱动扫描后重试成功")
    void transientFailureRetriesAndEventuallySucceeds() {
        service.receive(request("evt-transient", "order.paid", "TRANSIENT_2"));

        EventStatusView retrying = service.query("evt-transient");
        assertEquals("RETRYING", retrying.status());
        assertEquals(1, retrying.attempts());
        assertNotNull(retrying.nextRetryAt());
        assertEquals("DOWNSTREAM_UNAVAILABLE", retrying.failureReason());
        assertEquals(0, service.queryFailed().size(), "RETRYING 不算失败区");

        // 未到退避时间：扫描不应提前入队
        assertEquals(0, service.dispatchDueRetries(System.currentTimeMillis()));

        // 时间推进到退避点之后：扫描排队第 2 次（仍在前 N 次失败窗口内），再扫描第 3 次（恢复）
        int queued = service.dispatchDueRetries(Long.MAX_VALUE / 2);
        assertEquals(1, queued);
        assertEquals("RETRYING", service.query("evt-transient").status(), "第2次仍临时失败");
        assertEquals(2, service.query("evt-transient").attempts());

        service.dispatchDueRetries(Long.MAX_VALUE / 2);
        EventStatusView success = service.query("evt-transient");
        assertEquals("SUCCESS", success.status());
        assertEquals(3, success.attempts());
        assertTrue(landingLog.hasLanded("evt-transient"), "最终落地一次");
    }

    @Test
    @DisplayName("用例6：持续不可用耗尽 maxAttempts 后进入失败区，原因 DOWNSTREAM_UNAVAILABLE")
    void permanentFailureEntersFailureZoneAfterRetriesExhausted() {
        service.receive(request("evt-perm", "x", "PERMANENT"));
        assertEquals("RETRYING", service.query("evt-perm").status());

        service.dispatchDueRetries(Long.MAX_VALUE / 2); // 第2次
        assertEquals("RETRYING", service.query("evt-perm").status());

        service.dispatchDueRetries(Long.MAX_VALUE / 2); // 第3次 = maxAttempts
        EventStatusView failed = service.query("evt-perm");
        assertEquals("FAILED", failed.status());
        assertTrue(failed.inFailureZone());
        assertEquals("DOWNSTREAM_UNAVAILABLE", failed.failureReason());
        assertEquals(3, failed.attempts());
        assertNotNull(failed.failureMessage());
        assertFalse(failed.failureMessage().toLowerCase().contains("exception"),
                "对外失败说明不得包含底层异常字样");
        assertFalse(landingLog.hasLanded("evt-perm"));
    }

    @Test
    @DisplayName("用例7：同一事件并发投递50次——只落地一次，attempts=终态尝试数，duplicateCount=49")
    void concurrentDuplicatesLandExactlyOnce() throws Exception {
        int threads = 50;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    EventResponse resp = service.receive(request("evt-race", "order.created", null));
                    if (resp.duplicate()) {
                        duplicates.incrementAndGet();
                    } else {
                        accepted.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(1, accepted.get(), "只有一个请求被视为首次接收");
        assertEquals(threads - 1, duplicates.get());
        EventStatusView view = service.query("evt-race");
        assertEquals("SUCCESS", view.status());
        assertEquals(1, view.attempts(), "下游只被实际调用并落地一次");
        assertEquals(threads - 1, view.duplicateCount());
        assertTrue(landingLog.hasLanded("evt-race"));
    }

    @Test
    @DisplayName("用例8：进入 FAILED 终态后再次驱动处理，状态不回退、attempts 不增长")
    void terminalFailureStateNeverRollsBack() {
        service.receive(request("evt-terminal", "x", "PERMANENT"));
        service.dispatchDueRetries(Long.MAX_VALUE / 2);
        service.dispatchDueRetries(Long.MAX_VALUE / 2);
        assertEquals("FAILED", service.query("evt-terminal").status());

        // 包级方法直接驱动，模拟任何“迟到”的重试动作
        service.processOnce("evt-terminal");
        service.processOnce("evt-terminal");

        EventStatusView view = service.query("evt-terminal");
        assertEquals("FAILED", view.status());
        assertEquals(3, view.attempts());
        assertEquals(FailureReason.DOWNSTREAM_UNAVAILABLE.name(), view.failureReason());
    }
}
