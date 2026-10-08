package com.github.feng111627.idempotenteventingestionjava.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * 测试覆盖日志扩展：把每个测试类的 {@code @DisplayName}（业务名）与
 * 每个用例的 {@code @DisplayName}（用例名）及其结果打印到测试日志。
 * 运行 {@code mvn test} 即可直观看出“覆盖了哪些业务、各业务有哪些用例、结果如何”。
 */
public class BusinessCoverageLogger
        implements TestWatcher, BeforeEachCallback, BeforeAllCallback, AfterAllCallback {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger("业务覆盖");

    @Override
    public void beforeAll(ExtensionContext context) {
        log.info("======= 业务测试组开始：{} =======", businessName(context));
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        log.info("▶ 执行用例 [{}] {}", businessName(context), context.getDisplayName());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        log.info("  ✔ 用例通过：{}", context.getDisplayName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        log.error("  ✘ 用例失败：{} —— {}", context.getDisplayName(),
                cause == null ? "" : cause.toString());
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        log.warn("  ⚠ 用例中止：{}", context.getDisplayName());
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        log.info("  ○ 用例跳过：{}（原因={}）", context.getDisplayName(), reason.orElse("未说明"));
    }

    @Override
    public void afterAll(ExtensionContext context) {
        log.info("======= 业务测试组结束：{} =======", businessName(context));
    }

    private String businessName(ExtensionContext context) {
        return context.getTestClass()
                .map(c -> {
                    DisplayName d = c.getAnnotation(DisplayName.class);
                    return d == null ? c.getSimpleName() : d.value();
                })
                .orElse("未知业务");
    }
}
