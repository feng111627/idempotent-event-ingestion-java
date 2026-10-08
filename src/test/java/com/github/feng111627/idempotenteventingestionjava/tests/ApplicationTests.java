package com.github.feng111627.idempotenteventingestionjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 应用上下文冒烟测试。
 */
@SpringBootTest(properties = "ingestion.store.journal-path=target/smoke-journal.jsonl")
public class ApplicationTests {
    @Test
    public void contentLoad() {
    }
}
