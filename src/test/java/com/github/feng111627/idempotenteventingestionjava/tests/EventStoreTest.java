package com.github.feng111627.idempotenteventingestionjava.tests;

import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;
import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.ingest.EventStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("事件存储：去重注册、状态一致性与重启恢复")
@ExtendWith(ScenarioLoggerExtension.class)
class EventStoreTest {

    @TempDir
    Path tempDir;

    private EventStore newStore() {
        return new EventStore(tempDir.resolve("journal.jsonl").toString());
    }

    private EventRecord record(String id, EventStatus status) {
        EventRecord r = new EventRecord();
        r.setEventId(id);
        r.setPayload("payload");
        r.setStatus(status);
        r.setCreatedAt(Instant.now());
        r.setUpdatedAt(Instant.now());
        return r;
    }

    @Test
    @DisplayName("同一 eventId 只能注册一次，重复注册返回首个处理结果")
    void registerOnlyOnce() {
        EventStore store = newStore();
        CompletableFuture<EventRecord> first = new CompletableFuture<>();
        assertThat(store.registerIfAbsent(record("e1", EventStatus.RECEIVED), first)).isNull();

        CompletableFuture<EventRecord> second = new CompletableFuture<>();
        CompletableFuture<EventRecord> existing =
                store.registerIfAbsent(record("e1", EventStatus.RECEIVED), second);
        assertThat(existing).isSameAs(first);
        assertThat(store.find("e1").orElseThrow().getDuplicateAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("状态不允许回退，终态不可再变更")
    void statusNeverRegresses() {
        EventStore store = newStore();
        EventRecord r = record("e2", EventStatus.RECEIVED);
        store.registerIfAbsent(r, new CompletableFuture<>());

        r.setStatus(EventStatus.PROCESSING);
        store.update(r);

        EventRecord regressed = record("e2", EventStatus.RECEIVED);
        assertThatThrownBy(() -> store.update(regressed))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("回退");

        EventRecord delivered = record("e2", EventStatus.DELIVERED);
        store.update(delivered);
        EventRecord again = record("e2", EventStatus.PROCESSING);
        store.update(again); // 终态变更被忽略而不是报错
        assertThat(store.find("e2").orElseThrow().getStatus()).isEqualTo(EventStatus.DELIVERED);
    }

    @Test
    @DisplayName("重启后从本地日志恢复状态，不丢事件、不重复计数")
    void restartRestoresState() {
        EventStore store = newStore();
        EventRecord a = record("a", EventStatus.RECEIVED);
        store.registerIfAbsent(a, new CompletableFuture<>());
        a.setStatus(EventStatus.DELIVERED);
        a.setAttempts(1);
        store.update(a);

        EventRecord b = record("b", EventStatus.RECEIVED);
        store.registerIfAbsent(b, new CompletableFuture<>());
        store.incrementDuplicateAttempts("b");
        store.incrementDuplicateAttempts("b");

        // 模拟进程重启：用同一日志路径新建存储
        EventStore restarted = newStore();
        assertThat(restarted.findAll()).hasSize(2);
        assertThat(restarted.find("a").orElseThrow())
                .extracting(EventRecord::getStatus, EventRecord::getAttempts)
                .containsExactly(EventStatus.DELIVERED, 1);
        assertThat(restarted.find("b").orElseThrow().getDuplicateAttempts()).isEqualTo(2);

        // 重启后的重复投递能拿到一致视图，且重复计数继续累加而不是重置
        CompletableFuture<EventRecord> existing =
                restarted.registerIfAbsent(record("a", EventStatus.RECEIVED), new CompletableFuture<>());
        assertThat(existing).isNotNull();
        assertThat(existing.join().getStatus()).isEqualTo(EventStatus.DELIVERED);
        assertThat(restarted.find("a").orElseThrow().getDuplicateAttempts()).isEqualTo(1);
    }
}
