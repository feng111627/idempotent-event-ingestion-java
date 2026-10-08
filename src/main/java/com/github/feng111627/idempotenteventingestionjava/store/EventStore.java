package com.github.feng111627.idempotenteventingestionjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * 事件记录存储（本地 JSON 文件实现，每条事件一个文件）。
 *
 * <p>一致性约定：</p>
 * <ul>
 *   <li>每个 eventId 一把内存锁（{@link ConcurrentHashMap#compute} 保证锁对象唯一），
 *       {@link #computeIfAbsent} / {@link #update} 内的读-改-写整体持锁，
 *       并发投递只创建一条记录、重复计数不丢失；</li>
 *   <li>写盘走“写临时文件 + 原子 move”，进程崩溃不会留下半写 JSON，重启可安全恢复；</li>
 *   <li>所有对外返回的对象都是深拷贝，调用方无法绕过锁篡改落盘状态。</li>
 * </ul>
 */
@Component
public class EventStore {

    private static final Logger log = LoggerFactory.getLogger(EventStore.class);
    private static final String SUFFIX = ".json";

    private final Path dir;
    private final ObjectMapper objectMapper;
    /** 每 eventId 的锁对象，全程驻留（eventId 数量本地演示级别，无需回收）。 */
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public EventStore(AppProperties properties, ObjectMapper objectMapper) {
        this.dir = Path.of(properties.dataDir());
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(dir);
            log.info("事件存储初始化完成，目录={}", dir);
        } catch (IOException e) {
            throw new UncheckedIOException("初始化事件存储目录失败: " + dir, e);
        }
    }

    private Object lockOf(String eventId) {
        return locks.computeIfAbsent(eventId, k -> new Object());
    }

    private Path fileOf(String eventId) {
        // eventId 来自外部，规整文件名防止路径穿越
        String safe = eventId.replaceAll("[^a-zA-Z0-9._-]", "_");
        return dir.resolve(safe + SUFFIX);
    }

    /**
     * 原子地“存在则取出并计一次重复，不存在则用工厂创建”。
     * 整个检查-读-改-写在同一把 eventId 锁内完成。
     */
    public StoreResult computeIfAbsent(String eventId, Function<String, EventRecord> factory) {
        synchronized (lockOf(eventId)) {
            Path file = fileOf(eventId);
            if (Files.exists(file)) {
                EventRecord existing = readLocked(file);
                existing.setDuplicateCount(existing.getDuplicateCount() + 1);
                existing.setUpdatedAt(System.currentTimeMillis());
                writeLocked(file, existing);
                log.info("重复投递被识别 eventId={}, 累计重复次数={}", eventId, existing.getDuplicateCount());
                return new StoreResult(copy(existing), true);
            }
            EventRecord created = factory.apply(eventId);
            if (created == null) {
                throw new IllegalStateException("工厂未创建事件记录: " + eventId);
            }
            writeLocked(file, created);
            log.info("新事件已接收 eventId={}", eventId);
            return new StoreResult(copy(created), false);
        }
    }

    /** 加锁读-改-写单条记录；记录不存在时抛出 {@link IllegalStateException}。 */
    public EventRecord update(String eventId, UnaryOperator<EventRecord> mutator) {
        synchronized (lockOf(eventId)) {
            Path file = fileOf(eventId);
            if (!Files.exists(file)) {
                throw new IllegalStateException("事件记录不存在: " + eventId);
            }
            EventRecord current = readLocked(file);
            EventRecord next = mutator.apply(current);
            writeLocked(file, next);
            return copy(next);
        }
    }

    /** 只读快照（深拷贝）；不存在返回 null。 */
    public EventRecord find(String eventId) {
        synchronized (lockOf(eventId)) {
            Path file = fileOf(eventId);
            return Files.exists(file) ? copy(readLocked(file)) : null;
        }
    }

    /** 全部事件的只读快照，按接收时间排序，用于列表查询与启动恢复。 */
    public List<EventRecord> findAll() {
        List<EventRecord> all = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.toString().endsWith(SUFFIX)).forEach(p -> {
                synchronized (lockOf(idFromFile(p))) {
                    all.add(copy(readLocked(p)));
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("读取事件列表失败", e);
        }
        all.sort(Comparator.comparingLong(EventRecord::getReceivedAt));
        return all;
    }

    /** 处于失败区（FAILED 终态）的记录。 */
    public List<EventRecord> findFailed() {
        return findAll().stream().filter(r -> r.getStatus() == EventStatus.FAILED).toList();
    }

    private String idFromFile(Path p) {
        String name = p.getFileName().toString();
        return name.substring(0, name.length() - SUFFIX.length());
    }

    // ---- 持锁调用的低层 IO ----

    private EventRecord readLocked(Path file) {
        try {
            byte[] bytes = Files.readAllBytes(file);
            return objectMapper.readValue(bytes, EventRecord.class);
        } catch (IOException e) {
            throw new UncheckedIOException("读取事件记录失败: " + file, e);
        }
    }

    /** 写临时文件后原子 move，杜绝半写文件被其他线程/重启读到。 */
    private void writeLocked(Path target, EventRecord record) {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp-" + Thread.currentThread().getId());
        try {
            byte[] bytes = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsBytes(record);
            Files.write(tmp, bytes);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicNotSupported) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写入事件记录失败: " + target, e);
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // 临时文件清理失败不影响主流程
            }
        }
    }

    /** 通过 JSON 往返做深拷贝，隔离外部引用与落盘对象。 */
    private EventRecord copy(EventRecord source) {
        try {
            return objectMapper.treeToValue(objectMapper.valueToTree(source), EventRecord.class);
        } catch (IOException e) {
            throw new UncheckedIOException("拷贝事件记录失败", e);
        }
    }

    /**
     * computeIfAbsent 的返回：record 为深拷贝；duplicate=true 表示命中已有记录。
     */
    public record StoreResult(EventRecord record, boolean duplicate) {
    }
}
