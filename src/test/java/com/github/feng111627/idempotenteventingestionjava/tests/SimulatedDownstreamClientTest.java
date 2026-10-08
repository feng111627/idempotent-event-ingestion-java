package com.github.feng111627.idempotenteventingestionjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.downstream.DeliveryResult;
import com.github.feng111627.idempotenteventingestionjava.downstream.DownstreamException;
import com.github.feng111627.idempotenteventingestionjava.downstream.LandingLog;
import com.github.feng111627.idempotenteventingestionjava.downstream.SimulatedDownstreamClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：本地模拟下游落地端的校验、故障注入与第二道落地去重。
 */
@DisplayName("业务二：模拟下游落地端（校验/故障注入/落地去重）")
class SimulatedDownstreamClientTest {

    private Path tmp;
    private ObjectMapper objectMapper;
    private LandingLog landingLog;
    private SimulatedDownstreamClient client;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        this.tmp = tempDir;
        this.objectMapper = new ObjectMapper();
        AppProperties props = new AppProperties(
                tempDir.resolve("events").toString(),
                tempDir.resolve("downstream").toString(),
                3, 200, 5000, false, 200);
        this.landingLog = new LandingLog(props);
        landingLog.init();
        this.client = new SimulatedDownstreamClient(objectMapper, landingLog);
    }

    private EventRecord record(String id, String payloadJson) {
        EventRecord r = new EventRecord(id, payloadJson, 3, System.currentTimeMillis());
        r.setStatus(EventStatus.PROCESSING);
        return r;
    }

    @Test
    @DisplayName("用例1：合法事件首次 LANDED，再投一次下游返回 ALREADY_LANDED 且不重复落地")
    void validEventLandsExactlyOnceAtDownstream() {
        EventRecord r = record("e-ok", "{\"type\":\"order.created\"}");
        r.setAttempts(0);
        assertEquals(DeliveryResult.LANDED, client.deliver(r));

        r.setAttempts(1);
        assertEquals(DeliveryResult.ALREADY_LANDED, client.deliver(r));
        assertTrue(landingLog.hasLanded("e-ok"));
    }

    @Test
    @DisplayName("用例2：缺少 type 字段 → 不可重试的校验失败，原因 VALIDATION_FAILED")
    void missingTypeIsNonRetryableValidationFailure() {
        EventRecord r = record("e-bad", "{\"foo\":\"bar\"}");
        DownstreamException ex = assertThrows(DownstreamException.class, () -> client.deliver(r));
        assertFalse(ex.isRetryable(), "校验失败不应重试");
        assertEquals(FailureReason.VALIDATION_FAILED, ex.getReason());
        assertFalse(landingLog.hasLanded("e-bad"), "校验失败不得落地");
    }

    @Test
    @DisplayName("用例3：payload 不是 JSON 对象 → 校验失败且不透出底层异常")
    void nonObjectPayloadIsValidationFailure() {
        EventRecord r = record("e-arr", "[1,2,3]");
        DownstreamException ex = assertThrows(DownstreamException.class, () -> client.deliver(r));
        assertEquals(FailureReason.VALIDATION_FAILED, ex.getReason());
        assertFalse(ex.getMessage().contains("JsonMappingException"), "对外信息不应包含底层异常类名");
    }

    @Test
    @DisplayName("用例4：TRANSIENT_2 前两次不可用（可重试），第三次恢复落地")
    void transientFailureRecoversAfterTwoAttempts() throws Exception {
        String payload = objectMapper.writeValueAsString(
                objectMapper.createObjectNode().put("type", "order.paid").put("_failMode", "TRANSIENT_2"));
        EventRecord r = record("e-transient", payload);

        r.setAttempts(1);
        DownstreamException first = assertThrows(DownstreamException.class, () -> client.deliver(r));
        assertTrue(first.isRetryable());
        assertEquals(FailureReason.DOWNSTREAM_UNAVAILABLE, first.getReason());

        r.setAttempts(2);
        DownstreamException second = assertThrows(DownstreamException.class, () -> client.deliver(r));
        assertTrue(second.isRetryable());

        r.setAttempts(3);
        assertEquals(DeliveryResult.LANDED, client.deliver(r));
    }

    @Test
    @DisplayName("用例5：PERMANENT 始终不可用，每次都报可重试的 DOWNSTREAM_UNAVAILABLE")
    void permanentFailureAlwaysUnavailableAndNeverLands() {
        EventRecord r = record("e-perm", "{\"type\":\"x\",\"_failMode\":\"PERMANENT\"}");
        for (int i = 0; i < 5; i++) {
            r.setAttempts(i);
            DownstreamException ex = assertThrows(DownstreamException.class, () -> client.deliver(r));
            assertTrue(ex.isRetryable());
            assertEquals(FailureReason.DOWNSTREAM_UNAVAILABLE, ex.getReason());
        }
        assertFalse(landingLog.hasLanded("e-perm"));
    }
}
