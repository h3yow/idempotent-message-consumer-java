# 本地幂等消息消费组件（Local Idempotent Message Consumer）

用内存 + 本地文件快照模拟消息中间件，在**重复投递、失败重试、并发消费、服务重启**四类情形下，
保证消息最终状态一致且**可解释**：每一次关键判定都产生一条带编码的审计轨迹（`TraceEvent`），
同时以 `MQ-TRACE` 前缀打印到日志。

- 运行时形态：`LocalMessageBroker`（核心）+ `ConsumerWorker`（消费线程）+ `FileBrokerStore`（JSON 快照）
- 持久化：每次状态变更后全量写 `localmq.state-file`，先写 `.tmp` 再原子 `rename`
- 无外部中间件依赖，Spring Boot 自动装配，也可直接 new 出来使用

---

## 1. 消费协议（Delivery Protocol）

1. **入队**：`enqueue(Message)`。`messageId` 即业务幂等键；消息按主题追加到每个订阅组的队尾，
   组内分配从 0 开始的连续位点 `offset`。不同消费者组各自维护位点与生效状态（广播式独立消费）。
2. **租约（lease）投递**：`poll(group, consumerId)` 非阻塞返回一个 `LeasedMessage`，带唯一 `leaseId`、
   本次尝试序号 `attemptNo` 与到期时间 `leaseExpiresAt`。
3. **确认**：
   - `acknowledgeSuccess(lease)`：业务生效，消息进入 `SUCCEEDED` 终态；
   - `acknowledgeFailure(lease, true, reason)`：可重试失败，进入退避；
   - `acknowledgeFailure(lease, false, reason)`：不可重试失败，直接死信。
   - 处理器抛异常 = 可重试失败（原因记为 `HANDLER_EXCEPTION:<异常类型>`）。
4. **租约超时**：`poll` 前系统回收所有过期租约，追加 `LEASE_TIMEOUT` 记录后消息重新可投递。
5. **迟到确认无效**：确认时校验 `leaseId` 与当前租约一致且状态为 `IN_FLIGHT`，否则记
   `LEASE_STALE_IGNORED`，该确认的业务效果被丢弃——**旧租约的迟到 nack 不能把成功状态改回失败**。

消息在组内的状态机：

```
PENDING ──grant lease──▶ IN_FLIGHT ──success──────────────▶ SUCCEEDED (终态)
                            │                                    │
                            ├──retryable nack──▶ RETRY_WAIT ──到期 poll──▶ IN_FLIGHT
                            ├──retryable nack（达到 maxAttempts）─────────▶ DEAD (终态)
                            ├──non-retryable nack────────────────────────▶ DEAD (终态)
                            ├──lease timeout / 重启未确认 ──▶ PENDING（重投）
                                                            
运维动作：DEAD ──redeliverDeadLetter──▶ PENDING（人工重投，只多一次机会）
           SUCCEEDED ──replayFrom(回退)──▶ PENDING（进度重放，业务幂等仍生效一次）
```

## 2. 幂等与去重规则（Idempotency & Dedup）

| 情形 | 判定编码 | 行为 |
|---|---|---|
| 相同 `messageId` 再次入队 | `ENQUEUE_DUPLICATED_GLOBAL_ID` | 忽略，返回**首次**入队的原始消息，不新增位点 |
| 已 `SUCCEEDED` 的消息再 poll | （不再产生租约；位点已提交） | 不投递，处理器不会被再次调用 |
| 持旧租约的迟到 ack/nack | `LEASE_STALE_IGNORED` | 丢弃，终态不被污染 |

保证的边界：**同一消费者组内同一条消息的成功结论只生效一次**。组之间互不影响
（同一条消息在两个组各生效一次，符合发布/订阅语义）。去重表即持久化快照中的组状态，
重启后仍然有效。

## 3. 重试与死信语义（Retry & Dead-Letter）

- 最大尝试次数 `maxAttempts`（**含首次投递**），按组配置。
- 退避策略 `BackoffPolicy`：默认指数退避
  `delay = min(base * multiplier^(n-1) + jitter, max)`；退避窗口内 poll 跳过该消息。
- 进入死信的两条路径，原因**可区分**：
  - `MAX_ATTEMPTS_REACHED`：可重试失败累计达到 `maxAttempts`（租约超时占用尝试次数）；
  - `NON_RETRYABLE_BY_HANDLER`：处理器显式判定不可重试，一次即死信。
- 死信记录 `DeadLetter` 保留**原始消息**与**全部尝试记录**（`AttemptRecord`：尝试序号、消费者、
  起止时间、结果、原因、异常详情），原始记录不丢失。`DEAD` 与 `SUCCEEDED` 同为终态，
  位点可以安全地越过死信继续前进。

每次尝试结果（`AttemptResult`）：`SUCCESS` / `RETRYABLE_FAILURE` / `NON_RETRYABLE_FAILURE`
/ `LEASE_TIMEOUT`。

## 4. 位点提交规则（Offset Commit）

语义：`committedOffset = k` 表示 **所有 offset ≤ k 的消息都已到达终态**，其前不存在未确认消息。

- **只进不退**：提交小于当前位点 → `OFFSET_COMMIT_REJECTED_ROLLBACK`；
- **不得跨越未确认消息**：`(committed, target]` 区间内存在 `PENDING/IN_FLIGHT/RETRY_WAIT`
  → `OFFSET_COMMIT_REJECTED_GAP`（拒绝信息中给出第一个缺口位点及其状态）；
- **不得越界**：target 超过最大位点 → `OFFSET_COMMIT_REJECTED_RANGE`；
- 三种拒绝均抛 `OffsetCommitRejectedException`（携带编码与判定依据），位点不变。
- broker 在消息到达终态时自动把位点推进到“连续终态水位”（`OFFSET_COMMITTED`）；
  并发多消费者乱序确认时，位点只推进连续前缀，因此**永远不会越过尚未确认的消息**。
- 重启后位点从快照恢复，未确认消息重新投递；它们再次到达终态后位点才继续推进。

## 5. 重启恢复（Restart Recovery）

构造 `LocalMessageBroker` 时加载快照：

- 快照中 `IN_FLIGHT` 的消息视为“进程死亡、未确认”，追加一条
  `LEASE_TIMEOUT / RECOVERED_AFTER_RESTART_UNACKED` 轨迹（占用当前尝试次数）后重置为
  `PENDING`，立即可被重新投递；
- `SUCCEEDED/DEAD` 终态、`committedOffset`、死信与全部历史轨迹原样保留；
- 因此重投不会产生第二次业务生效，且“为什么重投”在轨迹中可追溯。

**重启与尝试额度的一致性**：已消耗的尝试次数（`attemptCount`）持久化在快照里，重启后继续算数。
若宕机时最后一次租约恰好是第 `maxAttempts` 次尝试且未确认，重启后**不会**发放第
`maxAttempts + 1` 次租约——`poll` 在发放租约前检查有效尝试上限，额度耗尽的消息直接以
`MAX_ATTEMPTS_REACHED` 进入死信。重启既不多给投递机会，也不突破配置的重试上限。

## 6. 死信重投（Dead-Letter Redelivery，人工补偿）

`broker.redeliverDeadLetter(group, messageId)` 把已进死信的消息放回**原消费者组**再处理一遍，
用于人工补偿（如修复下游故障后补单）。

语义与边界：

- **同一条消息的延续，不是新消息**：`offset`、`messageId`、全部尝试轨迹与 `attemptCount`
  原样保留，重投后的投递 `attemptNo` 继续递增；不会产生新位点、新消息。
- **业务生效仍最多一次**：重投走正常租约投递与确认路径，组内“成功只生效一次”的约束不变。
- **原死信记录不丢**：`DeadLetter` 记录（含重投前全部尝试）原样保留；重投后再次耗尽的，
  **追加**一条新死信记录，历史不被抹掉。
- **重试上限仍然有效**：每次人工重投在 `maxAttempts` 之外**恰好多给一次**投递机会
  （有效上限 = `maxAttempts + redeliveryCount`）。重投后再失败立即回到死信，
  不会因为有人工重投就永远有下一次机会。
- **与自动重试可区分**：轨迹依次出现 `REDELIVERED_FROM_DEAD_LETTER`（人为放回，含第几次重投、
  已消耗尝试数）→ `REDELIVER_DISPATCHED`（重投投递）→ `ACK_SUCCEEDED` / `NACK_RETRY_EXHAUSTED`
  （最终结果）；租约上 `deliveryCause=REDELIVER`，处理器侧也能感知。
- **拒绝原因可区分**：组内无此消息 → `REDELIVER_REJECTED_UNKNOWN_MESSAGE`；
  消息不是死信终态 → `REDELIVER_REJECTED_NOT_DEAD`（携带当前状态）。
  均抛 `RedeliverRejectedException`，被拒绝时状态、位点、死信记录全部原样。

## 7. 消费进度重放（Offset Replay）

`broker.replayFrom(group, targetOffset)` 把目标组的消费进度**回退**到更早位置，
此后已处理过的消息会被重新投递，用于排查问题或重算结果。

语义与边界：

- **只影响目标组**：其他消费者组的位点与状态完全不变。
- **只回退、不前进**：`targetOffset` 必须 `<` 当前已提交位点且在 `[-1, maxOffset]` 内。
  越界 → `REPLAY_REJECTED_RANGE`；等于或大于当前位点 → `REPLAY_REJECTED_NOT_BACKWARD`。
  均抛 `ReplayRejectedException`，被拒绝时不留半截进度（校验全部通过后才改状态，
  且在同一把锁内一次持久化）。
- **重放可区分**：区间内 `SUCCEEDED` 的消息被重置为待投递，再次投递时轨迹为
  `REPLAY_DISPATCHED`、租约 `deliveryCause=REPLAY`、`attemptNo` 续增；
  业务侧凭 `messageId` 幂等去重，**生效仍最多一次**。
- **不越过未确认消息**：区间内 `IN_FLIGHT` 的消息**不被重置**，进行中的确认照常生效，
  不与回退互相覆盖；`DEAD` 消息保持死信（要复活请用死信重投）。
  返回值 `ReplayResult` 摘要包含重置条数、保留的在途/死信条数。
- **与位点规则兼容**：回退把 `committedOffset` 设为目标值，之后仍按“连续终态水位”前进；
  显式 `commitOffset` 的回退拒绝规则不变（运维回退只能走 `replayFrom`，留审计轨迹）。

## 8. 与并发消费共存

重投与重放都允许在同组消费者并发运行期间执行：

- 所有状态变更（含这两个运维动作）在同一把锁内完成并立即持久化，
  不会与进行中的 ack/nack 互相覆盖——租约校验（`leaseId` + `IN_FLIGHT`）对运维路径同样生效；
- 重放不重置在途消息，因此不会“偷走”任何一个待确认消息的处理结果；
- 操作被拒绝时抛异常且零副作用，可安全重试。

## 9. 配置项（`application.properties`，前缀 `localmq`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `localmq.enabled` | `true` | 是否自动装配 broker |
| `localmq.state-file` | `./data/localmq-state.json` | JSON 快照文件路径 |
| `localmq.default-max-attempts` | `3` | 参考默认最大尝试次数 |
| `localmq.default-lease-seconds` | `10` | 参考默认租约时长（秒） |
| `localmq.backoff-base-millis` | `50` | 退避基数 |
| `localmq.backoff-multiplier` | `2.0` | 退避乘数 |
| `localmq.backoff-max-millis` | `5000` | 退避上限 |
| `localmq.backoff-jitter-millis` | `25` | 随机抖动上限 |

> 每个组的实际参数以 `registerGroup(new ConsumerGroupConfig(group, topic, maxAttempts,
> leaseDuration, backoffPolicy))` 传入为准；properties 中的默认值用于应用层统一参考。
>
> 死信重投与进度重放**没有新增配置项**：重投的额外机会固定为“每次重投多一次”，
> 重放不需要开关；两者的约束都复用组级 `maxAttempts` 与位点规则。

## 10. 使用示例

```java
// 1) 注册组（重启后需用同名重新注册，位点从快照恢复）
broker.registerGroup(new ConsumerGroupConfig("order-group", "orders", 3,
        Duration.ofSeconds(10), new ExponentialBackoff(50, 2.0, 5000)));

// 2) 启动并发消费者（不同 consumerId）
new ConsumerWorker(broker, "order-group", "c-1", msg -> {
    // 业务；抛异常 => 可重试失败；return ConsumeOutcome.dead("...") => 直接死信
    return ConsumeOutcome.success();
}).start();

// 3) 生产
EnqueueResult r = broker.enqueue(new Message(UUID.randomUUID().toString(),
        "orders", "payload", System.currentTimeMillis()));

// 4) 观察判定依据
broker.getTrace();                 // 全部审计轨迹
broker.lastTraceFor(group, id);    // 某消息最后一条判定（忽略/失败原因）
broker.getCommittedOffset(group);
broker.getDeadLetters();

// 5) 运维动作：死信重投 / 进度重放（均可在消费者运行期间执行）
broker.redeliverDeadLetter("order-group", messageId);  // 人工补偿，原死信记录保留
ReplayResult r = broker.replayFrom("order-group", 42); // 回退到 42，之后消息重放
```

## 11. 测试覆盖（结论可复现）

| 测试类 | 场景 | 关键断言 |
|---|---|---|
| `DuplicateDeliveryTest` | 重复入队、成功后重复消费、迟到旧租约 nack | 业务只生效 1 次；三种忽略编码可区分 |
| `RetryAndDeadLetterTest` | 可重试失败耗尽、不可重试即死信（固定时钟+零退避） | 尝试次数精确、`cause` 可区分、原始消息保留、位点越过死信 |
| `ConcurrentConsumeOffsetTest` | 4 worker × 60 消息并发；回退/缺口/越界提交 | 每条生效 1 次、位点恰好到末端；三类拒绝编码分别正确 |
| `RestartRecoveryTest` | 成功/未确认/待处理三种消息跨“宕机”重启 | 未确认重投不重复生效、保留 `RECOVERED_AFTER_RESTART_UNACKED` 轨迹 |
| `DeadLetterRedeliverTest` | 死信人工重投成功/再失败/两类拒绝 | `attemptNo` 续增、原死信保留、每次重投只多一次机会、拒绝编码可区分 |
| `ReplayOffsetTest` | 进度回退后重放、只影响目标组、IN_FLIGHT 不被越过、非法回退 | 重放可区分（`REPLAY_DISPATCHED`/`deliveryCause=REPLAY`）、生效仍一次、拒绝后进度原样 |
| `ConcurrentOpsConsistencyTest` | 消费者并发运行期间执行重放与重投 | 每条生效恰 1 次、位点最终到末端、无半截状态 |
| `RestartAttemptBudgetTest` | 宕机在最后一次尝试的租约期内 | 重启不多给机会、不突破 `maxAttempts`，耗尽即死信 |
| `RepeatableStateDirTest` | 同一状态目录、同一脚本连跑两遍 | 第二遍零新投递，位点/状态/死信/尝试次数与第一遍完全一致 |

测试日志通过标准输出 + `MQ-TRACE` 日志打印**输入、位点与判定依据**，运行：

```bash
mvn test
```

### 确认测试可反复跑

测试套件对“同一状态目录连跑两遍”是安全的，可直接验证：

```bash
mvn test && mvn test        # 连续两遍，结果必须一致（全绿）
```

- 场景测试一律使用 JUnit `@TempDir`，每次运行全新目录，互不污染；
- `AutoconfigureSmokeTest` 刻意使用固定状态文件 `target/smoke-state.json` 且不清理，
  断言全部相对于运行前位点——它本身就是“同目录反复跑”的回归用例；
- `RepeatableStateDirTest` 在同一快照文件上把完整脚本（入队→失败→死信→重投→成功）
  连演两遍，逐键比对两遍的最终状态。
