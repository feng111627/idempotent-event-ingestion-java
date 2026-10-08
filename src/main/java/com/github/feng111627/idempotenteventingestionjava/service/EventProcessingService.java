package com.github.feng111627.idempotenteventingestionjava.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.downstream.DeliveryResult;
import com.github.feng111627.idempotenteventingestionjava.downstream.DownstreamClient;
import com.github.feng111627.idempotenteventingestionjava.downstream.DownstreamException;
import com.github.feng111627.idempotenteventingestionjava.retry.RetryPolicy;
import com.github.feng111627.idempotenteventingestionjava.store.EventStore;
import com.github.feng111627.idempotenteventingestionjava.web.ApiException;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventRequest;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventResponse;
import com.github.feng111627.idempotenteventingestionjava.web.dto.EventStatusView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事件处理核心：接收去重、异步投递、失败退避重试、失败区与一致状态查询。
 *
 * <p>状态机（只进不退）：RECEIVED → PROCESSING ⇄ RETRYING → SUCCESS / FAILED。
 * SUCCESS、FAILED 为终态；终态记录的重复投递只返回当前状态，不再触发任何处理。</p>
 */
@Service
public class EventProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);

    private final EventStore store;
    private final DownstreamClient downstreamClient;
    private final RetryPolicy retryPolicy;
    private final AppProperties properties;
    private final ObjectMapper objectMapper;
    private final ExecutorService worker;

    /** 已排队/处理中的 eventId，防止扫描与接收路径重复入队。 */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public EventProcessingService(EventStore store,
                                  DownstreamClient downstreamClient,
                                  RetryPolicy retryPolicy,
                                  AppProperties properties,
                                  ObjectMapper objectMapper,
                                  @Qualifier("eventWorker") ExecutorService worker) {
        this.store = store;
        this.downstreamClient = downstreamClient;
        this.retryPolicy = retryPolicy;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.worker = worker;
    }

    /**
     * 接收事件：并发/重复投递只落地一次，重复请求可区分返回。
     *
     * @return 首次投递 httpStatusHint=202/duplicate=false；重复投递 200/duplicate=true
     */
    public EventResponse receive(EventRequest request) {
        if (request == null) {
            throw new ApiException(400, "BAD_REQUEST", "请求体不能为空");
        }
        String eventId = request.eventId();
        if (eventId == null || eventId.isBlank()) {
            throw new ApiException(400, "EVENT_ID_REQUIRED", "eventId 为必填项");
        }
        if (request.payload() == null || !request.payload().isObject()) {
            throw new ApiException(400, "PAYLOAD_INVALID", "payload 必须是 JSON 对象");
        }
        final String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(request.payload());
        } catch (JsonProcessingException e) {
            // 请求体本身无法序列化属于调用方问题，不暴露底层细节
            throw new ApiException(400, "PAYLOAD_INVALID", "payload 无法被解析为 JSON");
        }
        int maxAttempts = properties.maxAttempts();
        long now = System.currentTimeMillis();

        EventStore.StoreResult result = store.computeIfAbsent(eventId,
                id -> new EventRecord(id, payloadJson, maxAttempts, now));
        EventRecord record = result.record();

        if (!result.duplicate()) {
            // 首次接收：立即异步排队
            inFlight.add(eventId);
            worker.submit(() -> processOnce(eventId));
        } else {
            // 重复投递：若事件尚未在处理中（如刚启动还未恢复），尝试补一次排队
            tryEnqueueIfDue(record, now);
        }
        return toResponse(record, result.duplicate());
    }

    /**
     * 单次投递尝试（含状态机迁移、失败分类、退避调度），在工作线程执行。
     * public 便于单元测试直接驱动；外部直接调用同样安全（终态短路 + 单 eventId 锁）。
     */
    public void processOnce(String eventId) {
        try {
            // 终态保护：原子地把非终态推进到 PROCESSING 并累计 attempts；终态直接短路
            EventRecord claimed = store.update(eventId, r -> {
                if (r.getStatus() == EventStatus.SUCCESS || r.getStatus() == EventStatus.FAILED) {
                    return r;
                }
                r.setStatus(EventStatus.PROCESSING);
                r.setAttempts(r.getAttempts() + 1);
                r.setNextRetryAt(null);
                r.setUpdatedAt(System.currentTimeMillis());
                return r;
            });
            if (claimed.getStatus() != EventStatus.PROCESSING) {
                log.info("事件已处于终态，跳过处理 eventId={}, status={}", eventId, claimed.getStatus());
                return;
            }

            DeliveryResult delivery;
            try {
                delivery = downstreamClient.deliver(claimed);
            } catch (DownstreamException ex) {
                handleDownstreamFailure(eventId, claimed.getAttempts(), claimed.getMaxAttempts(), ex);
                return;
            } catch (RuntimeException unexpected) {
                // 任何未预期底层异常一律不外泄，按“下游暂时不可用”分类处理
                log.error("下游投递出现未预期异常 eventId={}", eventId, unexpected);
                handleDownstreamFailure(eventId, claimed.getAttempts(), claimed.getMaxAttempts(),
                        new DownstreamException(FailureReason.DOWNSTREAM_UNAVAILABLE, true,
                                "下游处理发生内部错误，将按退避策略重试"));
                return;
            }

            long finishAt = System.currentTimeMillis();
            store.update(eventId, r -> {
                r.setStatus(EventStatus.SUCCESS);
                r.setNextRetryAt(null);
                r.setFailureReason(null);
                r.setFailureMessage(null);
                r.setUpdatedAt(finishAt);
                r.setCompletedAt(finishAt);
                return r;
            });
            log.info("事件处理成功 eventId={}, 落地结果={}, 总尝试次数={}",
                    eventId, delivery, claimed.getAttempts());
        } catch (RuntimeException ex) {
            // 兜底：工作线程异常不能吞掉线程，记录完整堆栈于服务端，对外不暴露
            log.error("事件处理发生未预期错误 eventId={}", eventId, ex);
        } finally {
            inFlight.remove(eventId);
        }
    }

    /** 失败分类与状态迁移：校验失败立即进失败区；临时失败退避或耗尽后进失败区。 */
    private void handleDownstreamFailure(String eventId, int attempts, int maxAttempts,
                                         DownstreamException ex) {
        long now = System.currentTimeMillis();
        if (!ex.isRetryable()) {
            moveToFailureZone(eventId, ex.getReason(), ex.getMessage(), now);
            return;
        }
        if (attempts >= maxAttempts) {
            log.warn("重试已耗尽，事件进入失败区 eventId={}, attempts={}/{}",
                    eventId, attempts, maxAttempts);
            moveToFailureZone(eventId, FailureReason.DOWNSTREAM_UNAVAILABLE,
                    "下游持续不可用，已重试 " + attempts + " 次仍失败，进入失败区待人工/后续查询", now);
            return;
        }
        long nextAt = retryPolicy.nextRetryAt(attempts, now);
        store.update(eventId, r -> {
            r.setStatus(EventStatus.RETRYING);
            r.setNextRetryAt(nextAt);
            r.setFailureReason(FailureReason.DOWNSTREAM_UNAVAILABLE);
            r.setFailureMessage(ex.getMessage());
            r.setUpdatedAt(now);
            return r;
        });
        log.warn("下游临时失败，安排退避重试 eventId={}, attempts={}/{}, 下次重试≈{}ms 后",
                eventId, attempts, maxAttempts, nextAt - now);
    }

    private void moveToFailureZone(String eventId, FailureReason reason, String message, long now) {
        store.update(eventId, r -> {
            r.setStatus(EventStatus.FAILED);
            r.setFailureReason(reason);
            r.setFailureMessage(message);
            r.setNextRetryAt(null);
            r.setUpdatedAt(now);
            r.setCompletedAt(now);
            return r;
        });
        log.warn("事件进入失败区 eventId={}, reason={}", eventId, reason);
    }

    /** 重试扫描：捞起到期的 RETRYING 事件重新投递，返回本次排队数量。 */
    public int dispatchDueRetries(long now) {
        int queued = 0;
        for (EventRecord r : store.findAll()) {
            if (r.getStatus() == EventStatus.RETRYING
                    && r.getNextRetryAt() != null && r.getNextRetryAt() <= now
                    && tryEnqueue(r.getEventId())) {
                queued++;
            }
        }
        return queued;
    }

    /** 启动恢复：重启后把未完成（RECEIVED/PROCESSING/RETRYING）事件重新排队，不丢事件。 */
    public void recoverAfterRestart() {
        int recovered = 0;
        long now = System.currentTimeMillis();
        for (EventRecord r : store.findAll()) {
            if (r.getStatus() == EventStatus.RECEIVED || r.getStatus() == EventStatus.PROCESSING) {
                if (tryEnqueue(r.getEventId())) {
                    recovered++;
                }
            } else if (r.getStatus() == EventStatus.RETRYING
                    && r.getNextRetryAt() != null && r.getNextRetryAt() <= now
                    && tryEnqueue(r.getEventId())) {
                recovered++;
            }
        }
        log.info("启动恢复完成，重新排队未完成事件数={}", recovered);
    }

    /** 仅在事件“未在飞且当前应该被驱动”时入队；用于重复请求补排队。 */
    private void tryEnqueueIfDue(EventRecord snapshot, long now) {
        boolean due = switch (snapshot.getStatus()) {
            case RECEIVED, PROCESSING -> true;
            case RETRYING -> snapshot.getNextRetryAt() != null && snapshot.getNextRetryAt() <= now;
            case SUCCESS, FAILED -> false;
        };
        if (due) {
            tryEnqueue(snapshot.getEventId());
        }
    }

    /** 原子占位并异步处理；已在飞则返回 false。 */
    private boolean tryEnqueue(String eventId) {
        if (!inFlight.add(eventId)) {
            return false;
        }
        worker.submit(() -> processOnce(eventId));
        return true;
    }

    public EventStatusView query(String eventId) {
        EventRecord r = store.find(eventId);
        if (r == null) {
            throw new ApiException(404, "EVENT_NOT_FOUND", "未找到事件: " + eventId);
        }
        return toView(r);
    }

    public List<EventStatusView> queryAll() {
        return store.findAll().stream().map(this::toView).toList();
    }

    public List<EventStatusView> queryFailed() {
        return store.findFailed().stream().map(this::toView).toList();
    }

    private EventResponse toResponse(EventRecord r, boolean duplicate) {
        return new EventResponse(
                r.getEventId(),
                duplicate,
                duplicate ? 200 : 202,
                r.getStatus().name(),
                r.getAttempts(),
                r.getFailureReason() == null ? null : r.getFailureReason().name(),
                r.getFailureMessage(),
                r.getNextRetryAt(),
                r.getCompletedAt());
    }

    private EventStatusView toView(EventRecord r) {
        return new EventStatusView(
                r.getEventId(),
                r.getStatus().name(),
                r.getStatus() == EventStatus.FAILED,
                r.getAttempts(),
                r.getMaxAttempts(),
                r.getDuplicateCount(),
                r.getFailureReason() == null ? null : r.getFailureReason().name(),
                r.getFailureMessage(),
                r.getNextRetryAt(),
                r.getReceivedAt(),
                r.getUpdatedAt(),
                r.getCompletedAt());
    }
}
