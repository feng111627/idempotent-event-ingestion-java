# 本地幂等事件接入与投递（idempotent-event-ingestion-java）

一套**完全本地化**的事件接入 → 去重 → 异步投递 → 失败退避重试 → 失败区 → 状态查询演示实现。
接入端是本地 REST 服务，下游“落地端”是本地文件模拟的存储；**不依赖任何外部服务、中间件或真实凭据**，
`mvn test` / `java -jar` 即可完整体验重复投递、并发处理、下游失败后的最终结果。

---

## 1. 它解决什么问题

| 关注点 | 行为保证 |
| --- | --- |
| 重复投递（含并发同时到达） | 同一 `eventId` **只落地一次**；首次与重复请求用不同 HTTP 状态与响应字段区分，绝不静默丢弃 |
| 下游临时失败 | 按**指数退避 + 上限 + 可选抖动**自动重试，事件处于可查询的 `RETRYING` 态 |
| 下游持续失败 / 内容非法 | 进入独立**失败区**，失败原因可区分（校验失败 / 下游不可用），对外不暴露底层异常 |
| 状态一致查询 | 成功 / 重试中 / 失败 / 重复同一视图；并发下不重复计数、状态只进不退；进程重启不丢事件、不重复落地 |

---

## 2. 目录与组件

```
config/      AppProperties（全部本地参数）、AsyncConfig（投递线程池）、StartupLifecycle（启动恢复）
domain/      EventRecord（事件聚合根）、EventStatus、FailureReason
store/       EventStore：每 eventId 一把锁 + JSON 文件“临时文件+原子move”，并发读改写一致、重启可恢复
downstream/  DownstreamClient 接口；SimulatedDownstreamClient（校验/故障注入）；LandingLog（下游落地幂等区，第二道防线）
retry/       RetryPolicy（指数退避）、RetryScheduler（进程内周期扫描到期重试）
service/     EventProcessingService：接收去重、异步状态机、失败分类、退避、失败区、启动恢复、状态查询
web/         EventController、GlobalExceptionHandler、DTO
```

### 处理状态机（只进不退）

```
RECEIVED ──► PROCESSING ──成功──► SUCCESS（终态）
                 │  ▲
        临时失败 │  │ 到期扫描重投
                 ▼  │
              RETRYING
                 │
   校验失败(立即) / 临时失败重试耗尽
                 ▼
               FAILED（终态，进入失败区）
```

`SUCCESS`、`FAILED` 为终态：重复投递只返回现状并累加 `duplicateCount`，任何迟到的重试都会被短路。

### 两道幂等防线

1. **接入端去重**：`EventStore.computeIfAbsent` 在 eventId 锁内完成“存在即计重复、不存在即创建+排队”，
   并发请求只有一个能创建记录，其余全部计入 `duplicateCount`，不触发下游调用。
2. **下游落地去重**：模拟下游的 `LandingLog` 维护“已落地 eventId”文件集合，即便重启补偿/重复重放，
   同一事件在下游也只会写入一次（返回 `ALREADY_LANDED`）。

---

## 3. 接入约定（HTTP）

Base：`/api/events`，请求/响应均为 JSON。

### 3.1 投递事件 `POST /api/events`

请求体：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `eventId` | 是 | 业务幂等键，非空字符串，全局唯一 |
| `payload` | 是 | JSON 对象；必须含非空 `type` 字段（下游校验规则） |

`payload` 中保留字段 `_failMode`（仅本地演示用，用于故障注入）：

- `"PERMANENT"`：下游始终不可用，可重试，最终进失败区；
- `"TRANSIENT_N"`：前 N 次调用不可用（如 `"TRANSIENT_2"`），第 N+1 次起恢复。

响应 `EventResponse` 关键字段：`eventId`、`duplicate`、`httpStatusHint`、`status`、
`attempts`（实际下游调用次数）、`failureReason`、`failureMessage`、`nextRetryAt`、`completedAt`。

| 场景 | HTTP 状态 | 响应特征 |
| --- | --- | --- |
| 首次接收 | **202 Accepted** | `duplicate=false` |
| 重复投递（任何时机） | **200 OK** | `duplicate=true`，并带当前状态与累计重复次数 |
| `eventId` 缺失/空 | 400 | `code=EVENT_ID_REQUIRED` |
| `payload` 非 JSON 对象 | 400 | `code=PAYLOAD_INVALID` |
| 请求体不是合法 JSON | 400 | `code=BAD_REQUEST`（不回显 Jackson 细节） |

### 3.2 状态查询

- `GET /api/events/{eventId}`：单事件一致视图；不存在返回 404 `EVENT_NOT_FOUND`。
- `GET /api/events`：全部事件视图。
- `GET /api/events/failures`：**失败区**，仅 `FAILED` 终态事件。

视图 `EventStatusView`：`eventId, status, inFailureZone, attempts, maxAttempts, duplicateCount,
failureReason, failureMessage, nextRetryAt, receivedAt, updatedAt, completedAt`。

### 3.3 失败原因分类（FailureReason）

- `VALIDATION_FAILED`：事件内容非法（如缺 `type`）。**不可重试**，首次处理后直接进失败区。
- `DOWNSTREAM_UNAVAILABLE`：下游临时/持续不可用。可重试；达到 `maxAttempts` 后进失败区。
- 对外响应只含**原因码 + 可读中文说明**，底层异常类名/堆栈仅写服务端日志，不透出接口。

---

## 4. 去重与重试规则

### 去重

- 判重键：请求体顶层 `eventId`。
- 并发语义：按 eventId 串行化读-改-写；`duplicateCount` 精确反映被拦截的重复次数，
  `attempts` 只统计真正发起的下游调用次数（重复请求不增加该值）。
- 终态重复：成功/失败事件再次投递返回 200 + 当前状态，不触发任何处理，状态不回退。

### 重试与退避

- 仅对**可重试**失败（`DOWNSTREAM_UNAVAILABLE`）重试；校验失败立即入失败区。
- 退避公式：`delay_n = min(backoffBaseMillis × 2^(n-1), backoffMaxMillis)`，
  可选叠加 0~25% 随机抖动（避免重试雪崩）。默认 `base=200ms, max=5000ms`。
  序列示例：200 → 400 → 800 → 1600 → 3200 → 5000（封顶）。
- 最大尝试次数 `maxAttempts`（默认 3，**含首次**）；耗尽后置 `FAILED` +
  `DOWNSTREAM_UNAVAILABLE`，进入失败区。
- 调度：进程内单线程扫描器按固定周期把已到 `nextRetryAt` 的 `RETRYING` 事件重新入队；
  未到期不会提前重放。
- 重启恢复：启动时将 `RECEIVED/PROCESSING` 与已到期 `RETRYING` 事件重新排队；
  未到期 `RETRYING` 继续遵循原退避时间。事件文件与下游落地文件均持久化在本地磁盘。

---

## 5. 本地验证方法

### 5.1 运行单元/集成测试

```bash
mvn test
```

共 31 个用例、6 个业务测试组，全部无外部依赖。控制台会打印“业务测试组/用例/结果”覆盖日志，例如：

```
业务覆盖 - ======= 业务测试组开始：业务四：事件处理核心服务（去重/重试/失败区/状态机） =======
业务覆盖 - ▶ 执行用例 [...] 用例7：同一事件并发投递50次——只落地一次，attempts=终态尝试数，duplicateCount=49
业务覆盖 -   ✔ 用例通过：...
```

| 测试类 | 覆盖业务 |
| --- | --- |
| `RetryPolicyTest` | 退避序列指数增长与封顶、关闭抖动确定性、抖动范围 |
| `SimulatedDownstreamClientTest` | payload 校验、临时/永久故障注入、下游落地去重 |
| `EventStoreTest` | 100 并发只建一条、500 并发自增不丢更新、重启读回、快照隔离 |
| `EventProcessingServiceTest` | 首次/重复可区分、校验失败直入失败区、临时重试成功、重试耗尽、50 并发只落地一次、终态不回退 |
| `RecoveryTest` | 崩溃在 PROCESSING/RETRYING 后的重启恢复，不丢事件、不提前重放、不重复落地 |
| `EventControllerIntegrationTest` | 真实 Spring MVC + 真实扫描器：202/200/400/404、失败区两类原因、端到端重试 |

### 5.2 启动应用手动验证

```bash
mvn -DskipTests package
java -jar target/idempotent-event-ingestion-java-1.0-SNAPSHOT.jar --server.port=18099
# server.port=0 时随机端口；也可用 --server.port=xxxx 指定
```

另开终端：

```bash
# 首次 -> 202
curl -i -X POST localhost:18099/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-1","payload":{"type":"order.created"}}'

# 重复 -> 200, duplicate=true
curl -i -X POST localhost:18099/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-1","payload":{"type":"order.created"}}'

# 临时失败：前1次不可用，之后自动重试成功
curl -X POST localhost:18099/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-t","payload":{"type":"order.paid","_failMode":"TRANSIENT_1"}}'

# 持续失败：重试耗尽后进入失败区
curl -X POST localhost:18099/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-p","payload":{"type":"x","_failMode":"PERMANENT"}}'

# 校验失败：直接进入失败区（VALIDATION_FAILED）
curl -X POST localhost:18099/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"evt-b","payload":{"noType":"x"}}'

# 查询
curl localhost:18099/api/events/evt-1
curl localhost:18099/api/events/failures
curl localhost:18099/api/events
```

### 5.3 可配置参数（`application.properties`，均可被命令行覆盖）

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `app.ingestion.data-dir` | `${java.io.tmpdir}/idempotent-ingestion/events` | 事件 JSON 文件目录 |
| `app.ingestion.downstream-data-dir` | `${java.io.tmpdir}/idempotent-ingestion/downstream` | 模拟下游落地文件目录 |
| `app.ingestion.max-attempts` | `3` | 最大投递尝试次数（含首次） |
| `app.ingestion.backoff-base-millis` | `200` | 退避基数 |
| `app.ingestion.backoff-max-millis` | `5000` | 退避上限 |
| `app.ingestion.retry-jitter` | `true` | 是否启用抖动（测试中关闭以获得确定性） |
| `app.ingestion.scan-interval-millis` | `200` | 重试扫描周期 |

删除两个数据目录即可把系统重置为全新状态。

---

## 6. 边界与说明（本地演示定位）

- 存储为“每事件一个 JSON 文件 + 单进程内分段锁”，保证**单进程多线程**严格一致与**进程重启**恢复；
  它不是多实例分布式存储，多实例同时写同一目录不在本演示范围内。
- 异步投递使用进程内线程池，接收接口立即返回 202；最终状态通过查询接口获得。
- 所有故障均由请求中的 `_failMode` 在本地确定性地注入，便于重复验证。
