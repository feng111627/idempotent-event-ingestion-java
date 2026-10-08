package com.github.feng111627.idempotenteventingestionjava.ingest;

import com.github.feng111627.idempotenteventingestionjava.api.IngestRequest;
import com.github.feng111627.idempotenteventingestionjava.api.IngestResponse;
import com.github.feng111627.idempotenteventingestionjava.delivery.DeliveryService;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;
import com.github.feng111627.idempotenteventingestionjava.domain.IngestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * 事件接入：校验、去重、触发投递。
 * <p>
 * 同一 eventId 只处理一次；并发或重试的重复投递会等待首个处理完成，
 * 并收到可区分的 DUPLICATE 响应，不会重复写入下游。
 */
@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);
    private static final Pattern EVENT_ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final int MAX_PAYLOAD_LENGTH = 4096;

    private final EventStore store;
    private final DeliveryService deliveryService;

    public EventIngestionService(EventStore store, DeliveryService deliveryService) {
        this.store = store;
        this.deliveryService = deliveryService;
    }

    public IngestResponse ingest(IngestRequest request) {
        String eventId = request == null ? null : request.eventId();
        if (eventId == null || eventId.isBlank()) {
            throw new EventValidationException("eventId 不能为空");
        }
        String validationError = validate(request);
        if (validationError != null) {
            // 校验失败也落入失败区，便于查询（原因：VALIDATION_FAILED）
            EventRecord rejected = newRecord(request, EventStatus.DEAD_LETTER);
            rejected.setFailureReason(FailureReason.VALIDATION_FAILED);
            rejected.setFailureDetail(validationError);
            CompletableFuture<EventRecord> done = new CompletableFuture<>();
            CompletableFuture<EventRecord> existing = store.registerIfAbsent(rejected, done);
            if (existing == null) {
                done.complete(rejected);
                log.warn("event={} 校验失败：{}", eventId, validationError);
                return new IngestResponse(eventId, IngestResult.REJECTED, rejected.getStatus(), validationError);
            }
            EventRecord first = join(existing);
            return new IngestResponse(eventId, IngestResult.DUPLICATE, first.getStatus(),
                    "重复投递：该事件已被受理，当前状态 " + first.getStatus());
        }

        EventRecord record = newRecord(request, EventStatus.RECEIVED);
        CompletableFuture<EventRecord> processing = new CompletableFuture<>();
        CompletableFuture<EventRecord> existing = store.registerIfAbsent(record, processing);
        if (existing != null) {
            EventRecord first = join(existing);
            log.info("event={} 识别为重复投递（第 {} 次重复），返回首次处理结果 {}",
                    eventId, first.getDuplicateAttempts(), first.getStatus());
            return new IngestResponse(eventId, IngestResult.DUPLICATE, first.getStatus(),
                    "重复投递：该事件已被受理，当前状态 " + first.getStatus());
        }
        try {
            deliveryService.deliverWithRetry(record);
            processing.complete(record);
        } catch (RuntimeException e) {
            processing.completeExceptionally(e);
            throw e;
        }
        return new IngestResponse(eventId, IngestResult.ACCEPTED, record.getStatus(),
                record.getStatus() == EventStatus.DELIVERED ? "已投递" : "已受理，状态 " + record.getStatus());
    }

    private EventRecord newRecord(IngestRequest request, EventStatus status) {
        EventRecord record = new EventRecord();
        record.setEventId(request.eventId());
        record.setPayload(request.payload());
        record.setStatus(status);
        record.setCreatedAt(Instant.now());
        record.setUpdatedAt(record.getCreatedAt());
        return record;
    }

    private String validate(IngestRequest request) {
        if (!EVENT_ID.matcher(request.eventId()).matches()) {
            return "eventId 仅允许字母、数字、点、下划线、连字符，长度 1-128";
        }
        if (request.payload() == null || request.payload().isBlank()) {
            return "payload 不能为空";
        }
        if (request.payload().length() > MAX_PAYLOAD_LENGTH) {
            return "payload 长度超过 " + MAX_PAYLOAD_LENGTH;
        }
        return null;
    }

    private EventRecord join(CompletableFuture<EventRecord> future) {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("等待首次处理结果失败", e);
        }
    }
}
