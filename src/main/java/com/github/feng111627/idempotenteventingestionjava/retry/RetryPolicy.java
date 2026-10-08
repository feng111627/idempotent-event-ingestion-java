package com.github.feng111627.idempotenteventingestionjava.retry;

import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 指数退避重试策略：第 {@code n} 次失败后的等待 = min(base * 2^(n-1), max)，
 * 可选最多 25% 的随机抖动，避免大量失败事件在同一刻重试形成雪崩。
 */
@Component
public class RetryPolicy {

    private final AppProperties properties;

    public RetryPolicy(AppProperties properties) {
        this.properties = properties;
    }

    /**
     * 计算下一次允许重试的时间。
     *
     * @param failedAttempts 已经失败的尝试次数（1 表示首次调用刚失败，等待第 2 次）
     * @param now            当前 epoch millis
     * @return 下一次允许重试的时间 epoch millis
     */
    public long nextRetryAt(int failedAttempts, long now) {
        long delay = backoffMillis(failedAttempts);
        if (properties.retryJitter()) {
            // 0%~25% 的随机增量
            long jitter = ThreadLocalRandom.current().nextLong(delay / 4 + 1);
            delay += jitter;
        }
        return now + delay;
    }

    /**
     * 退避原始毫秒数（不含抖动）：base, base*2, base*4, ...，封顶 max。
     * 例如 base=200 时：200、400、800、1600、3200、5000、5000...
     */
    public long backoffMillis(int failedAttempts) {
        long base = properties.backoffBaseMillis();
        long max = properties.backoffMaxMillis();
        int shift = Math.max(0, failedAttempts - 1);
        long raw = shift >= 63 ? Long.MAX_VALUE : base << shift;
        // 移位溢出保护
        if (raw <= 0 || raw > max) {
            raw = max;
        }
        return raw;
    }
}
