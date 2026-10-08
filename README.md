# 幂等事件接入与投递（本地模拟）

一套纯本地的事件接入与投递流程：接入端接收事件并去重，投递端将事件落地到本地模拟的下游；
重复投递、并发处理与下游失败时的最终结果均可预期。**不依赖任何外部服务或真实凭据。**

## 架构概览

```
POST /api/events ──> EventIngestionService ──> EventStore（去重注册 + 状态机 + 本地日志持久化）
                          │                          │
                          ▼                          ▼
                   DeliveryService（重试 + 指数退避）   data/event-journal.jsonl（重启重放恢复）
                          │
                          ▼
              SimulatedDownstreamClient（本地模拟下游落地区）
```

## 接入约定

### 接收事件

```
POST /api/events
Content-Type: application/json

{"eventId": "order-123", "payload": "任意业务载荷，最长 4096 字符"}
```

- `eventId`：必填，全局唯一，仅允许字母、数字、`.`、`_`、`-`，长度 1–128。**同一 `eventId` 视为同一事件。**
- `payload`：必填，非空。

响应（`result` 字段可区分三种结果）：

| 场景 | HTTP | result | 说明 |
|---|---|---|---|
| 新事件，处理完成 | 200 | `ACCEPTED` | `status` 为最终状态（`DELIVERED` 或 `DEAD_LETTER`） |
| 重复投递（含并发） | 200 | `DUPLICATE` | 返回首次处理的结果状态，不会重复写入下游 |
| 校验失败 | 400 | `REJECTED` | 事件落入失败区，原因 `VALIDATION_FAILED` |
| 请求体非法 / 缺 eventId | 400 | — | 统一错误体 `{"code": "...", "message": "..."}`，不透出底层异常 |

### 状态查询

| 接口 | 说明 |
|---|---|
| `GET /api/events` | 全部事件的一致状态视图 |
| `GET /api/events/{eventId}` | 单事件状态（不存在返回 404） |
| `GET /api/events/failures` | 失败区（死信）：持续失败或校验失败的事件 |

状态视图字段：`status`、`attempts`（投递尝试次数）、`duplicateAttempts`（重复投递次数）、
`failureReason`、`failureDetail`、`createdAt`、`updatedAt`。

## 去重规则

- 同一 `eventId` 只被处理一次：存储层通过原子 `putIfAbsent` 注册，**并发到达的重复请求会等待首个处理完成**，
  随后收到 `DUPLICATE` 响应与首次处理的最终状态，不会被静默丢弃，也不会重复写入下游。
- 每次重复投递都会累加 `duplicateAttempts`，可在状态视图中查询；进程重启后计数不丢、不重置。
- 即使首次处理已进入死信（`DEAD_LETTER`），重复投递仍返回 `DUPLICATE`，不会触发重新投递。

## 状态机与一致性

```
RECEIVED → PROCESSING → (RETRYING →)* → DELIVERED（终态）
                                       → DEAD_LETTER（终态）
```

- 状态只允许单调推进，回退会被拒绝；终态不可再变更。
- 所有状态变更追加写入本地日志 `data/event-journal.jsonl`（可通过 `ingestion.store.journal-path` 配置），
  进程重启后重放恢复：不丢事件、不重复计数、状态不回退。

## 重试与退避规则

- 下游临时故障（`DownstreamUnavailableException`）按指数退避重试：
  第 n 次重试前等待 `initialBackoffMs × backoffMultiplier^(n-1)` 毫秒。
- 默认配置（`application.properties`）：`max-attempts=3`、`initial-backoff-ms=100`、`backoff-multiplier=2.0`，
  即退避序列为 100ms、200ms。
- 重试耗尽后事件进入失败区（`DEAD_LETTER`），`failureReason=DOWNSTREAM_UNAVAILABLE`。
- 校验失败不重试，直接落入失败区，`failureReason=VALIDATION_FAILED`。
- 失败原因可区分：`VALIDATION_FAILED` / `DOWNSTREAM_UNAVAILABLE` / `DUPLICATE_DELIVERY`（枚举预留）；
  底层异常只记录在服务端日志，对外接口只返回稳定错误码与消息。

## 本地模拟下游的失败注入约定

`SimulatedDownstreamClient` 按 payload 内容注入故障，便于本地验证：

| payload 包含 | 行为 |
|---|---|
| `TRANSIENT_FAIL:n` | 该事件前 n 次投递抛临时故障，之后成功（验证重试） |
| `PERMANENT_FAIL` | 该事件每次投递都失败（验证死信区） |
| 其他 | 直接落地成功 |

## 本地验证

### 运行单测

```bash
mvn test
```

日志中 `[TEST-COVERAGE]` 行会列出各业务场景及用例通过情况，覆盖：

- **事件存储**：去重注册、状态不回退、重启恢复不丢事件不重复计数
- **投递链路**：临时失败重试成功、指数退避规律、持续失败进死信区
- **事件接入**：重复投递去重、16 线程并发投递只受理一次、校验失败
- **对外接口**：响应可区分、状态一致查询、失败区查询、错误体不透出底层异常

### 手动验证（curl）

```bash
# 启动（默认 8080 端口）
mvn spring-boot:run

# 1. 正常事件
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","payload":"hello"}'

# 2. 重复投递 → result=DUPLICATE，且 GET /api/events/e1 中 duplicateAttempts=1
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e1","payload":"hello"}'

# 3. 临时失败 2 次后成功 → attempts=3，状态 DELIVERED
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e2","payload":"TRANSIENT_FAIL:2 data"}'

# 4. 持续失败 → 进入失败区
curl -s -X POST localhost:8080/api/events -H 'Content-Type: application/json' \
  -d '{"eventId":"e3","payload":"PERMANENT_FAIL data"}'
curl -s localhost:8080/api/events/failures

# 5. 重启进程后再次查询 e1/e2/e3：状态与计数保持不变（日志重放恢复）
curl -s localhost:8080/api/events
```
