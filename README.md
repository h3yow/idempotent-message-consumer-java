# 本地幂等消息消费组件（idempotent-message-consumer）

用**内存 / 本地文件**模拟消息中间件，在重复投递、失败重试、并发消费、服务重启四类场景下，
给出**一致且可解释**的最终状态。不依赖外部 MQ / 数据库，开箱即用。

- 技术栈：Java 21、Spring Boot 3.4.x，单进程内以一把可重入锁串行化所有状态变更。
- 存储：`InMemoryStateStore`（纯内存）或 `FileStateStore`（本地 JSON，原子写入，支持重启恢复）。

---

## 1. 快速开始

```java
// 1. 构建 broker（生产中由 Spring 自动注入单例，见第 6 节）
RetryPolicy policy = RetryPolicy.of(
        3,                       // maxAttempts：总投递次数上限
        Duration.ofMillis(100),  // 初始退避
        2.0,                     // 退避倍数
        Duration.ofSeconds(10),  // 退避上限
        Duration.ofSeconds(30)); // 投递租约
IdempotentBroker broker = new IdempotentBroker(new InMemoryStateStore(), policy);

// 2. 订阅消费者组：回调返回成功 / 可重试失败 / 不可重试失败
broker.subscribe("order-service", (messageId, payload) -> {
    // ...业务处理...
    return ConsumeOutcome.success();
    // return ConsumeOutcome.retryable("下游超时");
    // return ConsumeOutcome.nonRetryable("消息签名非法");
});

// 3. 入队（可带业务幂等键）
broker.enqueue("orders", "biz-id-1001", "create-order");

// 4. 拉取并消费（poll -> 执行业务回调 -> ack），返回每条消息的判定
var results = broker.consumeOnce("order-service", "orders", "consumer-1", 10);

// 5. 提交位点
OffsetCommitResult r = broker.commitOffset("order-service", "orders", 1);
```

也可以手动分两步：`poll(...)` 拿到 `Delivery` 句柄，处理后调用
`ack(group, deliveryId, outcome)`。**只有当前有效的投递句柄能确认消息。**

---

## 2. 消费协议（Delivery Protocol）

### 2.1 投递句柄 Delivery

- 每次派发都生成全局唯一的 `deliveryId`，并携带 `messageId / topic / offset / attemptNo / consumerId`。
- 同一时刻，一条消息在一个组内**至多有一个有效句柄**（在途租约保护）。
- 出现下列情况，旧句柄立即作废，用旧句柄确认会被拒绝：
  - 该句柄已被结算（成功 / 进死信）后被重放；
  - 消息重试中，旧句柄迟到确认；
  - broker 重启，所有重启前的在途句柄一律失效。

### 2.2 消息状态机（每个「组 × 消息」一份 `MessageRecord`）

```
PENDING ──派发──▶ INFLIGHT ──成功──────────────▶ SUCCEEDED(终态)
                     │
                     ├──可重试失败且仍有额度──▶ RETRYING ──退避到期派发──▶ INFLIGHT
                     │
                     └──可重试失败且耗尽额度 / 不可重试失败──▶ DEAD(死信, 终态)
```

### 2.3 消费回调的三种判定

| 返回 | 含义 | 后续 |
| --- | --- | --- |
| `ConsumeOutcome.success()` | 业务已生效 | 进入 `SUCCEEDED`，记录一次业务生效 |
| `ConsumeOutcome.retryable(reason)` | 临时性失败 | 未达上限则按退避重新投递，否则进死信 |
| `ConsumeOutcome.nonRetryable(reason)` | 永久性失败 | 不重试，直接进死信 |

回调抛出 `RuntimeException` 时，`consumeOnce` 会把它兜底成
`retryable("consumer threw ...")`，避免异常吞掉消息。

---

## 3. 幂等与去重规则

组件同时在**入端**和**消费端**保证幂等：

1. **入队去重（发布幂等）**：`enqueue(topic, messageId, payload)` 带相同 `messageId` 重复入队时，
   返回 `EnqueueResult(accepted=false, duplicate=true, 原 offset)`，不会写入第二条日志。
   不传 `messageId` 时由系统生成唯一 ID（此时不做跨消息去重）。
2. **同组生效恰好一次（消费幂等）**：业务状态只在某次投递被判定为 `success` 时改变，
   `MessageRecord.businessEffectCount` 在组内**恒为 0 或 1**。
   - 重复投递：已 `SUCCEEDED` 的消息不会再被 `poll` 出。
   - 重复消费 / 重放句柄：被忽略并返回**可区分原因** `IgnoreReason`：

| IgnoreReason | 触发情形 |
| --- | --- |
| `ALREADY_SUCCEEDED` | 消息此前已在本组成功，重放已结算成功的句柄 |
| `MESSAGE_IN_DLQ` | 消息已进入死信，重放其已结算句柄 |
| `UNKNOWN_DELIVERY` | 句柄不存在 / 不属于该组 / 重试期间旧句柄迟到 / **重启后旧句柄失效** |
| `IN_FLIGHT_ELSEWHERE` | （语义保留）消息正被其他消费者租约持有 |

3. **消费者组隔离（发布-订阅）**：位点、处理记录、业务生效次数都按「组」独立维护。
   同一条消息在每个组各自生效一次；同一组内只生效一次。

> 说明：业务回调只在 `consumeOnce`（成功）路径被调用；若手动 `poll` + `ack`，
> 组件保证“同一消息成功确认只发生一次”，业务自身也应基于 `messageId` 做落库幂等。

---

## 4. 重试与死信语义

- **总尝试次数 `maxAttempts`（含首次）**：例如 `maxAttempts=3` = 首次 + 最多 2 次重试；
  第 3 次仍失败即进入死信。
- **退避**：默认指数退避
  `delay(n) = min(backoffInitial × multiplier^(n-1), backoffMax)`，
  由 `BackoffStrategy` 接口支持自定义。未到 `nextVisibleAt` 的消息不会被 `poll` 出。
- **投递租约 `visibilityLease`**：消息派发后在租约期内对其他消费者不可见；
  租约到期仍未确认（消费者宕机、重启），下一次 `poll` 会作废旧句柄并重新投递。
  **租约过期重投不计入失败重试次数**（没有产生失败尝试记录）。
- **死信**：重试耗尽或不可重试失败进入 `DEAD`，可通过 `broker.deadLetters(group)` 查询。
  进入死信后：不再投递；原始消息、全部尝试记录、终止原因都保留，不污染已成功状态。

### 可追溯性（每次尝试的原因与结果）

每个 `MessageRecord` 只追加两类信息，永不改写历史：

- `attempts`：每次已结算的尝试 `DeliveryAttempt`
  （`deliveryId / consumerId / attemptNo / dispatchedAt / finishedAt / result / reason`）。
- `events`：生命周期事件（派发、退避调度、租约失效、重启恢复、死信原因等）。

死信终止原因 `terminalReason` 明确区分：
`retry budget exhausted after N attempt(s)...` 与 `non-retryable failure: ...`。

---

## 5. 位点提交规则

位点语义：`committedOffset = N` 表示该组在该 topic 上 **offset ∈ [0, N) 的消息已全部连续确认**
（`SUCCEEDED` 或 `DEAD` 都算“已终态”，因为都不会再投递）。

`commitOffset(group, topic, requested)` 的判定：

| 情形 | 结果 | 拒绝原因 `RejectReason` |
| --- | --- | --- |
| `requested < committed` | 拒绝（位点回退） | `REGRESSED` |
| `requested == committed` | 接受（幂等 no-op，不前移） | — |
| `[committed, requested)` 内存在未终态消息 | 拒绝（越过未确认消息 / 乱序提交） | `GAP_WITH_UNACKED`（明细列出阻塞消息及其状态） |
| `requested > 日志长度` | 拒绝（位点越界） | `UNKNOWN_MESSAGE` |
| 连续前缀全部终态 | 接受并前移 | — |

因此在并发消费下：**先完成的消息若 offset 更大，也不能越过尚未确认的前序消息提交位点**，
位点只能沿连续已确认前缀单调推进。

---

## 6. 服务重启恢复

- 每次状态变更（入队、派发、确认、位点提交）都会调用 `StateStore.save`；
  `FileStateStore` 采用「写临时文件 + 原子移动」，避免半写文件。
- 新进程用同一状态文件构造 broker 即完成恢复：
  - **已成功 / 已死信 / 已提交位点 / 历史尝试与事件** 原样保留；
  - **重启前在途未确认的消息**：构造时即作废旧句柄并恢复为可投递，
    下次 `poll` 重新派发（`attemptNo` 不增加，事件记录 `broker restarted: inflight lease recovered`）；
  - **退避中的消息**保留 `nextVisibleAt`，未到点仍不投递；
  - 旧句柄确认一律得到 `UNKNOWN_DELIVERY`，业务不会因重投而二次生效。

---

## 7. 配置项（Spring Boot）

组件提供自动装配，直接注入 `IdempotentBroker` 即可。前缀 `idempotent-mq`：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `idempotent-mq.max-attempts` | `3` | 总投递次数上限（含首次） |
| `idempotent-mq.backoff-initial` | `100ms` | 首次失败退避时长 |
| `idempotent-mq.backoff-multiplier` | `2.0` | 退避倍数 |
| `idempotent-mq.backoff-max` | `10s` | 退避上限 |
| `idempotent-mq.visibility-lease` | `30s` | 投递租约时长 |
| `idempotent-mq.state-file` | 空（纯内存） | 配置路径后启用本地 JSON 持久化与重启恢复 |

---

## 8. 并发模型与边界

- broker 内部以单把 `ReentrantLock` 串行化所有状态读写，`poll / ack / commitOffset`
  均为原子操作，多线程并发调用不会破坏状态。
- 时钟可注入（构造参数 `Clock`），测试用可变时钟确定性验证退避，生产用系统时钟。
- 适用范围：**单进程本地组件**（教学 / 单机幂等消费演示）。
  跨多实例的分布式锁、真实网络重传不在模拟范围内；接口与状态模型保留了向真实 MQ 迁移的边界。

---

## 9. 测试

测试位于 `src/test/.../tests/mq`，日志统一打印**输入、位点、判定依据**，结论确定可复现：

| 测试类 | 覆盖场景 |
| --- | --- |
| `DuplicateDeliveryTest` | 入队去重、成功只生效一次、重放句柄 `ALREADY_SUCCEEDED`、在途互斥 |
| `RetryAndDeadLetterTest` | 指数退避（未到点不投递）、`attemptNo` 递增、重试耗尽进死信、不可重试直达死信、旧句柄迟到被忽略 |
| `OffsetCommitTest` | 连续提交、`REGRESSED` / `GAP_WITH_UNACKED` / `UNKNOWN_MESSAGE` 拒绝、8 线程并发消费不重复生效 |
| `RestartRecoveryTest` | `FileStateStore` 重启：在途重投且不重复生效、位点持久化、死信与退避状态保留、旧句柄失效 |
| `ConsumerGroupTest` | 不同组各自生效一次、组间位点独立 |
| `SpringWiringTest` | Spring Boot 自动装配可用 |

运行：

```bash
mvn test
```
