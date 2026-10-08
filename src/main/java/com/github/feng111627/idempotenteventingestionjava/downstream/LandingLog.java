package com.github.feng111627.idempotenteventingestionjava.downstream;

import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 模拟下游落地区：已落地 eventId 集合（本地文本文件持久化，每行一个 id）。
 *
 * <p>作为第二道幂等防线：即便接入侧重试/重启补偿重复投递，下游也不会重复落地。
 * 启动时把文件全量读入内存集合；新落地以 append 方式持久化（行写在多数本地
 * 文件系统上对小写入是原子的），进程崩溃后已落地集合不丢失。</p>
 */
@Component
public class LandingLog {

    private static final Logger log = LoggerFactory.getLogger(LandingLog.class);

    private final Path file;
    private final Set<String> landed = ConcurrentHashMap.newKeySet();

    public LandingLog(AppProperties properties) {
        this.file = Path.of(properties.downstreamDataDir(), "landed-events.log");
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(file.getParent());
            if (Files.exists(file)) {
                landed.addAll(Files.readAllLines(file, StandardCharsets.UTF_8));
            } else {
                Files.createFile(file);
            }
            log.info("模拟下游落地区初始化完成，已落地事件数={}, 文件={}", landed.size(), file);
        } catch (IOException e) {
            throw new UncheckedIOException("初始化下游落地区失败: " + file, e);
        }
    }

    /**
     * 原子地标记已落地；首次标记返回 true，重复标记返回 false 且不重复写盘。
     */
    public boolean markLanded(String eventId) {
        boolean first = landed.add(eventId);
        if (first) {
            try {
                Files.writeString(file, eventId + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                // 写盘失败则回滚内存标记，保持内存/文件一致，并让上游按可重试失败处理
                landed.remove(eventId);
                throw new UncheckedIOException("下游落地持久化失败", e);
            }
        }
        return first;
    }

    public boolean hasLanded(String eventId) {
        return landed.contains(eventId);
    }

    public Set<String> landedIds() {
        return Set.copyOf(landed);
    }
}
