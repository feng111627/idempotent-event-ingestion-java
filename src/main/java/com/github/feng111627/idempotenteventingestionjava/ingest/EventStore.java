package com.github.feng111627.idempotenteventingestionjava.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事件存储：去重注册、状态单调推进与查询。
 * <p>
 * 通过本地追加式日志文件持久化（不依赖外部服务），进程重启后按日志重放恢复状态，
 * 保证不丢事件、不重复计数、状态不回退。
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);

    private final ConcurrentHashMap<String, EventRecord> records = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<EventRecord>> outcomes = new ConcurrentHashMap<>();
    /** 重复投递计数：独立于记录本身，避免注册与并发重复之间的竞态丢计数。 */
    private final ConcurrentHashMap<String, AtomicInteger> duplicateCounts = new ConcurrentHashMap<>();
    private final ObjectMapper mapper;
    private final Path journalPath;
    private final Object journalLock = new Object();

    public EventStore(@Value("${ingestion.store.journal-path:data/event-journal.jsonl}") String journalPath) {
        this.journalPath = Path.of(journalPath);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        replayJournal();
    }

    /**
     * 尝试注册新事件。
     *
     * @return null 表示注册成功（调用方负责处理并最终完成 processing future）；
     *         否则返回首个处理者的处理结果 future，调用方应等待其完成并返回重复响应。
     */
    public CompletableFuture<EventRecord> registerIfAbsent(EventRecord record, CompletableFuture<EventRecord> processing) {
        Objects.requireNonNull(record.getEventId(), "eventId must not be null");
        // 整体串行化：保证“注册”与“重复计数”之间没有竞态窗口
        synchronized (this) {
            CompletableFuture<EventRecord> existing = outcomes.putIfAbsent(record.getEventId(), processing);
            if (existing != null) {
                countDuplicate(record.getEventId());
                return existing;
            }
            record.setDuplicateAttempts(duplicateCounts
                    .computeIfAbsent(record.getEventId(), k -> new AtomicInteger()).get());
            records.put(record.getEventId(), copy(record));
            append(record);
            return null;
        }
    }

    /** 状态推进：只允许单调前进，终态（DELIVERED/DEAD_LETTER）不可再变更。 */
    public synchronized void update(EventRecord record) {
        EventRecord current = records.get(record.getEventId());
        if (current == null) {
            throw new IllegalStateException("event not registered: " + record.getEventId());
        }
        EventStatus from = current.getStatus();
        EventStatus to = record.getStatus();
        if (from == EventStatus.DELIVERED || from == EventStatus.DEAD_LETTER) {
            log.warn("event={} 已处于终态 {}，忽略到 {} 的变更", record.getEventId(), from, to);
            return;
        }
        if (to.ordinal() < from.ordinal()) {
            throw new IllegalStateException("状态不允许回退: " + from + " -> " + to);
        }
        record.setDuplicateAttempts(duplicateCounts
                .computeIfAbsent(record.getEventId(), k -> new AtomicInteger()).get());
        records.put(record.getEventId(), copy(record));
        append(record);
    }

    public Optional<EventRecord> find(String eventId) {
        EventRecord record = records.get(eventId);
        return record == null ? Optional.empty() : Optional.of(copy(record));
    }

    public List<EventRecord> findAll() {
        List<EventRecord> all = new ArrayList<>();
        records.values().forEach(r -> all.add(copy(r)));
        return all;
    }

    public synchronized void incrementDuplicateAttempts(String eventId) {
        countDuplicate(eventId);
    }

    /** 在持有实例锁的前提下累加重复计数并落盘。 */
    private void countDuplicate(String eventId) {
        int count = duplicateCounts.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
        EventRecord current = records.get(eventId);
        if (current != null) {
            current.setDuplicateAttempts(count);
            append(current);
        }
    }

    /** 防御性拷贝：内部状态与外部引用隔离，避免外部误改导致状态判断失真。 */
    private EventRecord copy(EventRecord r) {
        EventRecord c = new EventRecord();
        c.setEventId(r.getEventId());
        c.setPayload(r.getPayload());
        c.setStatus(r.getStatus());
        c.setAttempts(r.getAttempts());
        c.setDuplicateAttempts(r.getDuplicateAttempts());
        c.setFailureReason(r.getFailureReason());
        c.setFailureDetail(r.getFailureDetail());
        c.setCreatedAt(r.getCreatedAt());
        c.setUpdatedAt(r.getUpdatedAt());
        return c;
    }

    private void append(EventRecord record) {
        try {
            String line = mapper.writeValueAsString(record) + "\n";
            synchronized (journalLock) {
                Files.createDirectories(journalPath.toAbsolutePath().getParent());
                Files.writeString(journalPath, line, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写入事件日志失败", e);
        }
    }

    private void replayJournal() {
        if (!Files.exists(journalPath)) {
            return;
        }
        int restored = 0;
        try (var lines = Files.lines(journalPath, StandardCharsets.UTF_8)) {
            for (String line : (Iterable<String>) lines::iterator) {
                if (line.isBlank()) {
                    continue;
                }
                EventRecord record = mapper.readValue(line, EventRecord.class);
                EventRecord prev = records.get(record.getEventId());
                // 重放时同样禁止状态回退
                if (prev == null || record.getStatus().ordinal() >= prev.getStatus().ordinal()) {
                    records.put(record.getEventId(), record);
                }
                restored++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取事件日志失败", e);
        }
        // 重启后：为每条记录补一个已完成的 outcome，使重启后的重复投递能拿到一致视图；
        // 同时恢复重复计数，保证计数不丢、不重置。
        records.forEach((id, record) -> {
            outcomes.put(id, CompletableFuture.completedFuture(record));
            duplicateCounts.put(id, new AtomicInteger(record.getDuplicateAttempts()));
        });
        if (restored > 0) {
            log.info("从本地日志恢复 {} 条事件记录", restored);
        }
    }

}
