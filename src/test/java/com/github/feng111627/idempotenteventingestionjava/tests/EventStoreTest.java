package com.github.feng111627.idempotenteventingestionjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.store.EventStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：存储层在高并发接收下的“只建一条、重复计数不丢”，以及重启后状态不丢。
 */
@DisplayName("业务三：事件存储一致性（并发去重/原子更新/重启恢复）")
class EventStoreTest {

    private Path tempDir;
    private ObjectMapper objectMapper;
    private AppProperties props;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        this.tempDir = tempDir;
        this.objectMapper = new ObjectMapper();
        this.props = new AppProperties(
                tempDir.resolve("events").toString(),
                tempDir.resolve("downstream").toString(),
                3, 200, 5000, false, 200);
    }

    @Test
    @DisplayName("用例1：同一 eventId 并发 computeIfAbsent 100 次，只创建一条，duplicateCount=99")
    void concurrentComputeIfAbsentCreatesExactlyOneRecord() throws Exception {
        EventStore store = new EventStore(props, objectMapper);
        store.init();

        int threads = 100;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger firstHits = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    EventStore.StoreResult r = store.computeIfAbsent("evt-concurrent",
                            id -> new EventRecord(id, "{}", 3, System.currentTimeMillis()));
                    if (!r.duplicate()) {
                        firstHits.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发任务应在超时前完成");
        pool.shutdown();

        assertEquals(1, firstHits.get(), "恰好一个线程创建记录");
        EventRecord snapshot = store.find("evt-concurrent");
        assertNotNull(snapshot);
        assertEquals(threads - 1, snapshot.getDuplicateCount(), "其余 99 次必须全部计为重复");
        assertEquals(EventStatus.RECEIVED, snapshot.getStatus());
    }

    @Test
    @DisplayName("用例2：并发 update 递增 attempts 500 次，结果必须精确为 500（无丢失更新）")
    void concurrentUpdatesDoNotLoseIncrements() throws Exception {
        EventStore store = new EventStore(props, objectMapper);
        store.init();
        store.computeIfAbsent("evt-counter",
                id -> new EventRecord(id, "{}", 3, System.currentTimeMillis()));

        int times = 500;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(times);
        for (int i = 0; i < times; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    store.update("evt-counter", r -> {
                        r.setAttempts(r.getAttempts() + 1);
                        return r;
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(15, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(times, store.find("evt-counter").getAttempts(), "读-改-写原子性：计数不能丢");
    }

    @Test
    @DisplayName("用例3：模拟重启——新建的 store 实例能读回全部状态（含终态与失败原因）")
    void recordsSurviveStoreRestart() {
        EventStore first = new EventStore(props, objectMapper);
        first.init();
        first.computeIfAbsent("evt-a",
                id -> new EventRecord(id, "{}", 3, 1000L));
        first.update("evt-a", r -> {
            r.setStatus(EventStatus.SUCCESS);
            r.setAttempts(1);
            r.setCompletedAt(2000L);
            return r;
        });
        first.computeIfAbsent("evt/bad:id",
                id -> new EventRecord(id, "{}", 3, 1000L));

        // 全新实例 = 进程重启后内存为空，仅靠磁盘恢复
        EventStore second = new EventStore(props, objectMapper);
        second.init();
        EventRecord reloaded = second.find("evt-a");
        assertEquals(EventStatus.SUCCESS, reloaded.getStatus());
        assertEquals(1, reloaded.getAttempts());
        assertEquals(2000L, reloaded.getCompletedAt());
        // 特殊字符 eventId 做文件名规整后也应取回
        assertNotNull(second.find("evt/bad:id"));
        assertEquals(2, second.findAll().size());
    }

    @Test
    @DisplayName("用例4：find 返回深拷贝，外部修改不会污染落盘状态")
    void findReturnsDetachedCopy() {
        EventStore store = new EventStore(props, objectMapper);
        store.init();
        store.computeIfAbsent("evt-copy",
                id -> new EventRecord(id, "{}", 3, 1000L));
        EventRecord view = store.find("evt-copy");
        view.setAttempts(99);
        view.setStatus(EventStatus.FAILED);
        EventRecord again = store.find("evt-copy");
        assertEquals(0, again.getAttempts());
        assertEquals(EventStatus.RECEIVED, again.getStatus());
        assertFalse(again == view);
    }
}
