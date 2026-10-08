package com.github.feng111627.idempotenteventingestionjava.tests;

import com.github.feng111627.idempotenteventingestionjava.config.AppProperties;
import com.github.feng111627.idempotenteventingestionjava.retry.RetryPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 业务覆盖：下游临时失败后的重试与退避规则。
 */
@DisplayName("业务一：退避重试策略（可解释、有上限、可关闭抖动）")
class RetryPolicyTest {

    private AppProperties props(boolean jitter) {
        // dataDir 等参数与本测试无关，给默认值即可
        return new AppProperties(null, null, 3, 200, 5_000, jitter, 200);
    }

    @Test
    @DisplayName("用例1：退避序列按 base*2^(n-1) 指数增长并封顶 max")
    void backoffGrowsExponentiallyAndCapsAtMax() {
        RetryPolicy policy = new RetryPolicy(props(false));
        assertEquals(200, policy.backoffMillis(1));
        assertEquals(400, policy.backoffMillis(2));
        assertEquals(800, policy.backoffMillis(3));
        assertEquals(1600, policy.backoffMillis(4));
        assertEquals(3200, policy.backoffMillis(5));
        // base*2^5=6400 超过 5000，封顶
        assertEquals(5_000, policy.backoffMillis(6));
        assertEquals(5_000, policy.backoffMillis(20));
    }

    @Test
    @DisplayName("用例2：关闭抖动时，下次重试时间确定可预测（now+backoff）")
    void nextRetryAtIsDeterministicWithoutJitter() {
        RetryPolicy policy = new RetryPolicy(props(false));
        long now = 10_000;
        assertEquals(now + 200, policy.nextRetryAt(1, now));
        assertEquals(now + 400, policy.nextRetryAt(2, now));
    }

    @Test
    @DisplayName("用例3：开启抖动时，延迟不低于基础退避且不超过基础退避的1.25倍")
    void jitterStaysWithinQuarterOfBackoff() {
        RetryPolicy policy = new RetryPolicy(props(true));
        long now = 0;
        for (int failedAttempts = 1; failedAttempts <= 5; failedAttempts++) {
            long base = policy.backoffMillis(failedAttempts);
            long at = policy.nextRetryAt(failedAttempts, now);
            assertTrue(at >= base, "抖动不应缩短退避");
            assertTrue(at <= base + base / 4 + 1, "抖动不应超过基础退避的25%");
        }
    }
}
