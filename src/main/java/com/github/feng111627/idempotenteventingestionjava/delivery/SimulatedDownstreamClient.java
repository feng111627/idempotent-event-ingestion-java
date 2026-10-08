package com.github.feng111627.idempotenteventingestionjava.delivery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 本地模拟的下游落地端，不依赖任何外部服务或真实凭据。
 * <p>
 * 失败注入约定（便于本地验证）：
 * <ul>
 *   <li>payload 含 {@code TRANSIENT_FAIL:n} —— 该事件前 n 次投递抛临时故障，之后成功；</li>
 *   <li>payload 含 {@code PERMANENT_FAIL} —— 该事件每次投递都抛临时故障（用于验证死信区）；</li>
 *   <li>其余事件直接落地成功。</li>
 * </ul>
 */
@Component
public class SimulatedDownstreamClient implements DownstreamClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedDownstreamClient.class);
    private static final Pattern TRANSIENT = Pattern.compile("TRANSIENT_FAIL:(\\d+)");

    /** 已落地的载荷（落地区），key 为 eventId。 */
    private final Map<String, List<String>> landingZone = new ConcurrentHashMap<>();
    /** 每个事件剩余的临时失败次数。 */
    private final Map<String, AtomicInteger> transientFailuresRemaining = new ConcurrentHashMap<>();
    private final Map<String, Boolean> permanentFailure = new ConcurrentHashMap<>();

    @Override
    public void deliver(String eventId, String payload) {
        planFromPayload(eventId, payload);
        if (permanentFailure.getOrDefault(eventId, false)) {
            log.warn("下游模拟：event={} 永久故障，投递失败", eventId);
            throw new DownstreamUnavailableException("下游持续不可用（模拟永久故障）");
        }
        AtomicInteger remaining = transientFailuresRemaining.get(eventId);
        if (remaining != null && remaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            log.warn("下游模拟：event={} 临时故障，剩余注入失败次数={}", eventId, remaining.get());
            throw new DownstreamUnavailableException("下游暂时不可用（模拟临时故障）");
        }
        landingZone.computeIfAbsent(eventId, k -> new CopyOnWriteArrayList<>()).add(payload);
        log.info("下游模拟：event={} 落地成功，落地总数={}", eventId, deliveredCount(eventId));
    }

    /** 已成功落地的事件数（用于验证只落地一次）。 */
    public int deliveredCount(String eventId) {
        return landingZone.getOrDefault(eventId, List.of()).size();
    }

    public int totalDelivered() {
        return landingZone.values().stream().mapToInt(List::size).sum();
    }

    /** 测试/演示用：编程方式注入临时失败次数。 */
    public void planTransientFailures(String eventId, int times) {
        transientFailuresRemaining.put(eventId, new AtomicInteger(times));
    }

    private void planFromPayload(String eventId, String payload) {
        transientFailuresRemaining.computeIfAbsent(eventId, k -> {
            Matcher m = TRANSIENT.matcher(payload == null ? "" : payload);
            return m.find() ? new AtomicInteger(Integer.parseInt(m.group(1))) : new AtomicInteger(0);
        });
        if (payload != null && payload.contains("PERMANENT_FAIL")) {
            permanentFailure.putIfAbsent(eventId, true);
        }
    }
}
