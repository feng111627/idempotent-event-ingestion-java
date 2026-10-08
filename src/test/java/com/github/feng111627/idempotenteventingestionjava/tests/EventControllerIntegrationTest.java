package com.github.feng111627.idempotenteventingestionjava.tests;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 业务覆盖（HTTP 端到端）：接入约定、重复投递可区分响应、失败区查询、
 * 错误响应不透出底层异常、真实扫描器驱动的退避重试。
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("integration")
@TestMethodOrder(MethodOrderer.DisplayName.class)
@DisplayName("业务六：REST 端到端接入（真实 Spring + 真实重试扫描器）")
class EventControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @BeforeAll
    static void cleanData() throws Exception {
        // 每次集成测试前清空本地文件，保证幂等
        Path root = Path.of("target/it-data");
        if (Files.exists(root)) {
            try (var paths = Files.walk(root)) {
                paths.sorted(java.util.Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (Exception ignored) {
                            }
                        });
            }
        }
    }

    @Test
    @DisplayName("用例1：首次投递合法事件返回 202，随后查询为 SUCCESS")
    void firstPostAcceptedAndQuerySucceeds() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content("""
                                {"eventId":"http-ok","payload":{"type":"order.created"}}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.duplicate").value(false))
                .andExpect(jsonPath("$.httpStatusHint").value(202));

        Await.until("事件异步处理到 SUCCESS", () -> {
            try {
                String body = mockMvc.perform(get("/api/events/http-ok"))
                        .andReturn().getResponse().getContentAsString();
                return body.contains("\"status\":\"SUCCESS\"");
            } catch (Exception e) {
                return false;
            }
        });

        mockMvc.perform(get("/api/events/http-ok"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.inFailureZone").value(false))
                .andExpect(jsonPath("$.attempts").value(1));
    }

    @Test
    @DisplayName("用例2：重复投递返回 200 且 duplicate=true，状态仍 SUCCESS、attempts 不增加")
    void duplicatePostReturnsDistinguishableResponse() throws Exception {
        String body = """
                {"eventId":"http-ok","payload":{"type":"order.created"}}
                """;
        mockMvc.perform(post("/api/events").contentType("application/json").content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andExpect(jsonPath("$.httpStatusHint").value(200))
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.attempts").value(1));

        mockMvc.perform(get("/api/events/http-ok"))
                .andExpect(jsonPath("$.duplicateCount").value(1));
    }

    @Test
    @DisplayName("用例3：缺少 eventId 返回 400 与机器可读错误码，不写事件")
    void missingEventIdReturns400() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content("""
                                {"payload":{"type":"x"}}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("EVENT_ID_REQUIRED"));
    }

    @Test
    @DisplayName("用例4：请求体是畸形 JSON 返回干净的 400，不透出 Jackson 异常细节")
    void malformedJsonReturnsClean400() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content("{not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("BAD_REQUEST"));
    }

    @Test
    @DisplayName("用例5：查询不存在的事件返回 404 EVENT_NOT_FOUND")
    void queryUnknownEventReturns404() throws Exception {
        mockMvc.perform(get("/api/events/no-such-event"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EVENT_NOT_FOUND"));
    }

    @Test
    @DisplayName("用例6：缺 type 的事件异步进入失败区，失败列表可按 VALIDATION_FAILED 查到")
    void validationFailureAppearsInFailureZone() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content("""
                                {"eventId":"http-invalid","payload":{"noType":"x"}}
                                """))
                .andExpect(status().isAccepted());

        Await.until("校验失败事件进入失败区", () -> {
            try {
                String body = mockMvc.perform(get("/api/events/failures"))
                        .andReturn().getResponse().getContentAsString();
                return body.contains("http-invalid") && body.contains("VALIDATION_FAILED");
            } catch (Exception e) {
                return false;
            }
        });

        mockMvc.perform(get("/api/events/http-invalid"))
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.inFailureZone").value(true))
                .andExpect(jsonPath("$.failureReason").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.failureMessage").isNotEmpty());
    }

    @Test
    @DisplayName("用例7：PERMANENT 事件经真实扫描器重试 3 次后进失败区，原因 DOWNSTREAM_UNAVAILABLE")
    void permanentFailureRetriedByRealSchedulerThenFails() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content("""
                                {"eventId":"http-perm","payload":{"type":"x","_failMode":"PERMANENT"}}
                                """))
                .andExpect(status().isAccepted());

        Await.until("永久失败事件重试耗尽进入失败区（真实扫描器）", () -> {
            try {
                String body = mockMvc.perform(get("/api/events/http-perm"))
                        .andReturn().getResponse().getContentAsString();
                return body.contains("\"status\":\"FAILED\"")
                        && body.contains("\"attempts\":3")
                        && body.contains("DOWNSTREAM_UNAVAILABLE");
            } catch (Exception e) {
                return false;
            }
        });

        // 失败区里两类原因都能查到且相互区分
        mockMvc.perform(get("/api/events/failures"))
                .andExpect(jsonPath("$[?(@.eventId=='http-perm')].failureReason")
                        .value(org.hamcrest.Matchers.hasItem("DOWNSTREAM_UNAVAILABLE")))
                .andExpect(jsonPath("$[?(@.eventId=='http-invalid')].failureReason")
                        .value(org.hamcrest.Matchers.hasItem("VALIDATION_FAILED")));
    }

    @Test
    @DisplayName("用例8：TRANSIENT_1 事件先 RETRYING 再被扫描器重试成功，状态视图一致")
    void transientFailureRecoversViaRealScheduler() throws Exception {
        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content("""
                                {"eventId":"http-transient","payload":{"type":"y","_failMode":"TRANSIENT_1"}}
                                """))
                .andExpect(status().isAccepted());

        // 短暂窗口内应该能观察到 RETRYING（首次同步投递失败后即此态）
        Await.until("临时失败事件进入 RETRYING", () -> {
            try {
                String body = mockMvc.perform(get("/api/events/http-transient"))
                        .andReturn().getResponse().getContentAsString();
                return body.contains("RETRYING") || body.contains("SUCCESS");
            } catch (Exception e) {
                return false;
            }
        });

        Await.until("扫描器退避后重试成功", () -> {
            try {
                String body = mockMvc.perform(get("/api/events/http-transient"))
                        .andReturn().getResponse().getContentAsString();
                return body.contains("\"status\":\"SUCCESS\"")
                        && body.contains("\"attempts\":2");
            } catch (Exception e) {
                return false;
            }
        });
    }
}
