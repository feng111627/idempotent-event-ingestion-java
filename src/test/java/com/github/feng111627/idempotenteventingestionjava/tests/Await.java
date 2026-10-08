package com.github.feng111627.idempotenteventingestionjava.tests;

import java.time.Duration;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * 测试辅助：轮询等待断言成立，避免异步流程的脆弱 sleep。
 */
public final class Await {

    private Await() {
    }

    public static void until(String description, Supplier<Boolean> condition) {
        until(description, condition, Duration.ofSeconds(10));
    }

    public static void until(String description, Supplier<Boolean> condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        AssertionError last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.get()) {
                    return;
                }
            } catch (AssertionError e) {
                last = e;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("等待被中断: " + description);
            }
        }
        fail("等待超时: " + description + (last == null ? "" : "；最后断言: " + last.getMessage()));
    }
}
