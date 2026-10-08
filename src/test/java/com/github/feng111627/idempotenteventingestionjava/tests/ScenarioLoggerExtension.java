package com.github.feng111627.idempotenteventingestionjava.tests;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 在测试日志中打印每个业务场景及其用例执行结果，便于从日志确认覆盖范围。
 */
public class ScenarioLoggerExtension implements TestWatcher, BeforeAllCallback, AfterAllCallback {

    private final AtomicInteger passed = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    @Override
    public void beforeAll(ExtensionContext context) {
        log("==== 业务场景开始: %s ====", context.getDisplayName());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        passed.incrementAndGet();
        log("用例通过: %s", context.getDisplayName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        failed.incrementAndGet();
        log("用例失败: %s -> %s", context.getDisplayName(), cause.getMessage());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        log("==== 业务场景结束: %s，通过 %d，失败 %d ====",
                context.getDisplayName(), passed.get(), failed.get());
    }

    private void log(String format, Object... args) {
        System.out.printf("[TEST-COVERAGE] " + format + "%n", args);
    }
}
