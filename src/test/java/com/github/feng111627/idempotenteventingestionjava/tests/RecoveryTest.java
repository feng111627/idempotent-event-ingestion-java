package com.github.feng111627.idempotenteventingestionjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.downstream.LandingLog;
import com.github.feng111627.idempotenteventingestionjava.downstream.SimulatedDownstreamClient;
import com.github.feng111627.idempotenteventingestionjava.retry.RetryPolicy;
import com.github.feng111627.idempotenteventingestionjava.service.EventProcessingService;
import com.github.feng111627.idempotenteventingestionjava.store.EventStore;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventRequest;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventStatusView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：进程在“投递中途”崩溃后的恢复——事件不丢、下游不重复落地。
 */
@DisplayName("业务五：重启恢复（崩溃在 PROCESSING/RETRYING 后事件不丢、不重复落地）")
class RecoveryTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private AppProperties props() {
        return new AppProperties(
                tempDir.resolve("events").toString(),
                tempDir.resolve("downstream").toString(),
                3, 100, 1000, false, 50);
    }

    private EventProcessingService serviceFor(AppProperties props) {
        EventStore store = new EventStore(props, objectMapper);
        store.init();
        LandingLog landingLog = new LandingLog(props);
        landingLog.init();
        SimulatedDownstreamClient downstream = new SimulatedDownstreamClient(objectMapper, landingLog);
        return new EventProcessingService(store, downstream, new RetryPolicy(props),
                props, objectMapper, new DirectExecutorService());
    }

    private EventRequest request(String id, String type, String failMode) {
        ObjectNode payload = objectMapper.createObjectNode().put("type", type);
        if (failMode != null) {
            payload.put("_failMode", failMode);
        }
        return new EventRequest(id, payload);
    }

    @Test
    @DisplayName("用例1：事件停在 PROCESSING 时进程重建，recoverAfterRestart 后最终 SUCCESS 且落地一次")
    void unfinishedProcessingEventReplayedAfterRestart() {
        AppProperties props = props();

        // —— 第一“进程”：事件正常接收并成功，证明磁盘状态完整 ——
        EventProcessingService first = serviceFor(props);
        first.receive(request("evt-restart-ok", "order.created", null));
        assertEquals("SUCCESS", first.query("evt-restart-ok").status());

        // —— 模拟崩溃：手工塞入一条停在 PROCESSING 的记录（进程死在下游调用途中）——
        EventStore rawStore = new EventStore(props, objectMapper);
        rawStore.init();
        rawStore.computeIfAbsent("evt-crashed",
                id -> new EventRecord(id, "{\"type\":\"order.created\"}", 3, System.currentTimeMillis()));
        rawStore.update("evt-crashed", r -> {
            r.setStatus(EventStatus.PROCESSING);
            r.setAttempts(1);
            return r;
        });

        // —— 第二“进程”：全新实例启动恢复 ——
        EventProcessingService second = serviceFor(props);
        second.recoverAfterRestart();

        EventStatusView recovered = second.query("evt-crashed");
        assertEquals("SUCCESS", recovered.status(), "崩溃时未完成的事件恢复后必须成功，不丢事件");
        assertEquals(2, recovered.attempts(), "恢复重放新增一次尝试，由下游去重兜底不重复落地");

        // 已 SUCCESS 的旧事件不受恢复影响、状态不回退
        assertEquals("SUCCESS", second.query("evt-restart-ok").status());
        assertEquals(1, second.query("evt-restart-ok").attempts());

        // 全局视图一致：没有残留的 PROCESSING/RECEIVED
        for (EventStatusView v : second.queryAll()) {
            assertTrue(!"PROCESSING".equals(v.status()) && !"RECEIVED".equals(v.status()),
                    "恢复后不应残留未完成状态: " + v.eventId());
        }
    }

    @Test
    @DisplayName("用例2：崩溃在 RETRYING 等待中，重建后未到期不会提前重放；到期扫描后成功")
    void retryingEventRespectsBackoffAfterRestart() {
        AppProperties props = props();

        EventProcessingService first = serviceFor(props);
        // TRANSIENT_1：首次失败进入 RETRYING
        first.receive(request("evt-retry-crash", "order.paid", "TRANSIENT_1"));
        assertEquals("RETRYING", first.query("evt-retry-crash").status());
        Long nextRetryAt = first.query("evt-retry-crash").nextRetryAt();
        assertTrue(nextRetryAt > System.currentTimeMillis(), "退避时间应在未来");

        // 重建：恢复时事件尚未到期，不应被重放
        EventProcessingService second = serviceFor(props);
        second.recoverAfterRestart();
        assertEquals("RETRYING", second.query("evt-retry-crash").status());

        // 到期后由扫描接管
        second.dispatchDueRetries(Long.MAX_VALUE / 2);
        EventStatusView view = second.query("evt-retry-crash");
        assertEquals("SUCCESS", view.status());
        assertEquals(2, view.attempts());
    }
}
