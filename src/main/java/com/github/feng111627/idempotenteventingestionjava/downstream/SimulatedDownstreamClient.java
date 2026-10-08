package com.github.feng111627.idempotenteventingestionjava.downstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.UncheckedIOException;

/**
 * 本地模拟下游落地端，无需任何外部服务：
 * <ol>
 *   <li>校验 payload：必须是 JSON 对象且含非空 {@code type} 字段，否则为
 *       不可重试的 {@link FailureReason#VALIDATION_FAILED}；</li>
 *   <li>故障注入（写在 payload 的 {@code _failMode} 字段，仅用于本地演示）：
 *     <ul>
 *       <li>{@code PERMANENT}：每次调用都不可用，可重试，最终进失败区；</li>
 *       <li>{@code TRANSIENT_N}：前 N 次调用不可用（如 {@code TRANSIENT_2}），
 *           第 N+1 次起恢复，用于验证退避重试后成功；</li>
 *     </ul>
 *   </li>
 *   <li>落地去重：借助 {@link LandingLog} 保证同一 eventId 绝不重复落地
 *       （第二道幂等防线，重启补偿也命中）。</li>
 * </ol>
 */
@Component
public class SimulatedDownstreamClient implements DownstreamClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDownstreamClient.class);

    private final ObjectMapper objectMapper;
    private final LandingLog landingLog;

    public SimulatedDownstreamClient(ObjectMapper objectMapper, LandingLog landingLog) {
        this.objectMapper = objectMapper;
        this.landingLog = landingLog;
    }

    @Override
    public DeliveryResult deliver(EventRecord record) {
        JsonNode payload = parsePayload(record);
        validate(record.getEventId(), payload);

        // 当前是第几次尝试（1-based）：服务在调用前已把 attempts 自增为本次序号
        int thisAttempt = Math.max(1, record.getAttempts());
        applyFaultInjection(record.getEventId(), payload, thisAttempt);

        try {
            boolean first = landingLog.markLanded(record.getEventId());
            if (first) {
                log.info("下游落地成功 eventId={}, attempt={}", record.getEventId(), thisAttempt);
                return DeliveryResult.LANDED;
            }
            log.warn("下游去重命中，未重复落地 eventId={}, attempt={}", record.getEventId(), thisAttempt);
            return DeliveryResult.ALREADY_LANDED;
        } catch (UncheckedIOException e) {
            // 底层 IO 异常不外泄，归类为“下游暂时不可用”走重试
            log.warn("下游落地写入失败 eventId={}, attempt={}", record.getEventId(), thisAttempt, e);
            throw new DownstreamException(FailureReason.DOWNSTREAM_UNAVAILABLE, true,
                    "下游落地端暂时不可用，将按退避策略重试");
        }
    }

    private JsonNode parsePayload(EventRecord record) {
        try {
            return objectMapper.readTree(record.getPayload());
        } catch (Exception e) {
            throw new DownstreamException(FailureReason.VALIDATION_FAILED, false,
                    "事件负载不是合法 JSON");
        }
    }

    private void validate(String eventId, JsonNode payload) {
        if (payload == null || !payload.isObject()) {
            throw new DownstreamException(FailureReason.VALIDATION_FAILED, false,
                    "事件负载必须是 JSON 对象");
        }
        JsonNode type = payload.get("type");
        if (type == null || type.asText().isBlank()) {
            throw new DownstreamException(FailureReason.VALIDATION_FAILED, false,
                    "事件缺少必填字段 type 或其值为空");
        }
        log.debug("下游校验通过 eventId={}, type={}", eventId, type.asText());
    }

    private void applyFaultInjection(String eventId, JsonNode payload, int thisAttempt) {
        JsonNode modeNode = payload.get("_failMode");
        if (modeNode == null || modeNode.isNull()) {
            return;
        }
        String mode = modeNode.asText();
        if ("PERMANENT".equalsIgnoreCase(mode)) {
            log.warn("[故障注入] 下游永久不可用 eventId={}, attempt={}", eventId, thisAttempt);
            throw new DownstreamException(FailureReason.DOWNSTREAM_UNAVAILABLE, true,
                    "下游落地端暂时不可用，将按退避策略重试");
        }
        if (mode.regionMatches(true, 0, "TRANSIENT_", 0, "TRANSIENT_".length())) {
            int n;
            try {
                n = Integer.parseInt(mode.substring("TRANSIENT_".length()));
            } catch (NumberFormatException badMode) {
                throw new DownstreamException(FailureReason.VALIDATION_FAILED, false,
                        "_failMode 格式非法，应为 TRANSIENT_N（N 为非负整数）");
            }
            if (thisAttempt <= n) {
                log.warn("[故障注入] 下游临时不可用 eventId={}, attempt={}/{}-th-transient",
                        eventId, thisAttempt, n);
                throw new DownstreamException(FailureReason.DOWNSTREAM_UNAVAILABLE, true,
                        "下游落地端暂时不可用，将按退避策略重试");
            }
            log.info("[故障注入] 下游已恢复 eventId={}, attempt={}", eventId, thisAttempt);
        }
    }
}
