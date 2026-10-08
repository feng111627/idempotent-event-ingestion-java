package com.github.feng111627.idempotenteventingestionjava.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("对外接口：接入响应可区分、状态一致查询与错误体不透出")
@SpringBootTest
@AutoConfigureMockMvc
@ExtendWith(ScenarioLoggerExtension.class)
class EventControllerTest {

    @Autowired
    MockMvc mvc;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        Path dir = Files.createTempDirectory("controller-test");
        registry.add("ingestion.store.journal-path", () -> dir.resolve("journal.jsonl").toString());
        registry.add("ingestion.retry.initial-backoff-ms", () -> "1");
    }

    @Test
    @DisplayName("新事件：POST 返回 ACCEPTED，GET 状态视图一致为 DELIVERED")
    void ingestAndQueryConsistentView() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"api-1\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value("ACCEPTED"))
                .andExpect(jsonPath("$.status").value("DELIVERED"));

        mvc.perform(get("/api/events/api-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DELIVERED"))
                .andExpect(jsonPath("$.attempts").value(1))
                .andExpect(jsonPath("$.duplicateAttempts").value(0));
    }

    @Test
    @DisplayName("重复投递：响应 result=DUPLICATE 可区分，重复计数可见")
    void duplicateResponseIsDistinguishable() throws Exception {
        String body = "{\"eventId\":\"api-2\",\"payload\":\"hello\"}";
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value("ACCEPTED"));
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value("DUPLICATE"))
                .andExpect(jsonPath("$.status").value("DELIVERED"));

        mvc.perform(get("/api/events/api-2"))
                .andExpect(jsonPath("$.duplicateAttempts").value(1));
    }

    @Test
    @DisplayName("并发投递同一事件：接口层只有一个 ACCEPTED，状态视图不重复计数")
    void concurrentIngestViaApi() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                go.await();
                return mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                                .content("{\"eventId\":\"api-race\",\"payload\":\"x\"}"))
                        .andReturn().getResponse().getContentAsString();
            }));
        }
        go.countDown();
        List<String> bodies = new ArrayList<>();
        for (Future<String> f : futures) {
            bodies.add(f.get(30, TimeUnit.SECONDS));
        }
        pool.shutdown();

        assertThat(bodies.stream().filter(b -> b.contains("\"ACCEPTED\""))).hasSize(1);
        assertThat(bodies.stream().filter(b -> b.contains("\"DUPLICATE\""))).hasSize(threads - 1);

        mvc.perform(get("/api/events/api-race"))
                .andExpect(jsonPath("$.status").value("DELIVERED"))
                .andExpect(jsonPath("$.attempts").value(1))
                .andExpect(jsonPath("$.duplicateAttempts").value(threads - 1));
    }

    @Test
    @DisplayName("下游持续失败：事件进入失败区，可通过 /api/events/failures 查询且原因可区分")
    void deadLetterVisibleInFailures() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"api-dl\",\"payload\":\"PERMANENT_FAIL x\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DEAD_LETTER"));

        mvc.perform(get("/api/events/failures"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.eventId=='api-dl')].failureReason").value("DOWNSTREAM_UNAVAILABLE"));
    }

    @Test
    @DisplayName("校验失败：返回 400 与 VALIDATION_FAILED，失败区中原因可区分")
    void validationFailureResponse() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"bad id!\",\"payload\":\"x\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.result").value("REJECTED"));

        mvc.perform(get("/api/events/failures"))
                .andExpect(jsonPath("$[?(@.eventId=='bad id!')].failureReason").value("VALIDATION_FAILED"));
    }

    @Test
    @DisplayName("非法请求与未知事件：返回统一错误体/404，不透出底层异常")
    void errorsDoNotLeakInternals() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":null,\"payload\":\"x\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.exception").doesNotExist())
                .andExpect(jsonPath("$.stackTrace").doesNotExist());

        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));

        mvc.perform(get("/api/events/no-such-event"))
                .andExpect(status().isNotFound());
    }
}
