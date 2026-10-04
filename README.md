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
                            │
                            ├──retryable nack──▶ RETRY_WAIT ──到期 poll──▶ IN_FLIGHT
                            ├──retryable nack（达到 maxAttempts）─────────▶ DEAD (终态)
                            ├──non-retryable nack────────────────────────▶ DEAD (终态)
                            ├──lease timeout / 重启未确认 ──▶ PENDING（重投）
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

- **只进不退**：显式提交小于当前位点 → `OFFSET_COMMIT_REJECTED_ROLLBACK`；
  （水线唯一允许的回退是第 5b 节的显式运维操作 `rewindCommittedOffset`，与消费提交路径严格分开）
- **不得跨越未确认消息**：`(committed, target]` 区间内存在 `PENDING/IN_FLIGHT/RETRY_WAIT`
  → `OFFSET_COMMIT_REJECTED_GAP`（拒绝信息中给出第一个缺口位点及其状态）；
- **不得越界**：target 超过最大位点 → `OFFSET_COMMIT_REJECTED_RANGE`；
- 三种拒绝均抛 `OffsetCommitRejectedException`（携带编码与判定依据），位点不变。
- broker 在消息到达终态时自动把位点推进到“连续终态水位”（`OFFSET_COMMITTED`）；
  并发多消费者乱序确认时，位点只推进连续前缀，因此**永远不会越过尚未确认的消息**。
- 重启后位点从快照恢复，未确认消息重新投递；它们再次到达终态后位点才继续推进。

## 5. 重启恢复（Restart Recovery）

构造 `LocalMessageBroker` 后，**某个组第一次 `registerGroup` 时**按组执行恢复（恢复需要该组的
`maxAttempts` 配置）：

- 快照中 `IN_FLIGHT` 的消息视为“进程死亡、未确认”，追加一条
  `LEASE_TIMEOUT / RECOVERED_AFTER_RESTART_UNACKED` 轨迹（**占用当前尝试次数**）后重置为
  `PENDING`；
- **尝试额度账对齐**：回收占满最后一次尝试额度（`attemptCount >= maxAttempts`）的消息不再给新的
  投递机会，直接进死信（`MAX_ATTEMPTS_REACHED`）。重启不会重置 `attemptCount`、不多出投递机会、
  不突破配置的重试上限，也不会把已耗尽的消息无限重投；仍有剩余额度的消息才重新可投递；
- `SUCCEEDED/DEAD` 终态、`committedOffset`、死信、每次重放记录与全部历史轨迹原样保留；
- 因此重投不会产生第二次业务生效，且“为什么重投 / 为什么直接死信”在轨迹中可追溯。

## 5a. 人工死信重投（Manual Dead-Letter Replay）

用于**人工补偿**：把已进入死信的同一条消息作为延续（不是新消息）重新放回原消费者组再处理一遍。

```java
// 按位点定位（也可先从 getDeadLetters() 找到对应记录）
ReplayRecord r = broker.replayDeadLetter("order-group", 12,
        "ops-alice", "下游修复后的人工补偿");
// 之后消费者照常 poll，拿到的租约带重放标记
LeasedMessage lease = broker.poll("order-group", "c-1");
lease.deliveryKind(); // DEAD_LETTER_REPLAY
lease.replayNo();     // 这是该消息第 1 次被人工重放
lease.attemptNo();    // 延续历史序号（不是从 1 重新开始）
```

语义与边界：

- **同一条消息的延续**：messageId、offset、payload 与历史尝试记录全部不变；审计轨迹编码
  `DEAD_LETTER_REPLAY_ACCEPTED`（含 operator/reason/replayNo/历史尝试数），投递编码
  `REPLAY_DISPATCHED`（含 `attemptNo`），与自动重试的 `DISPATCHED` 明确区分。
- **原始死信不抹掉**：每次重放后再失败都会**新增**一条 `DeadLetter`（`replayNo >= 1`、
  携带 operator），原始死信 `replayNo=0` 永远保留；重放成功也不清理历史死信记录。
- **业务生效仍只算一次**：死信消息此前从未成功过，重放成功是它第一次也是唯一一次生效；
  `succeededOnce` 一旦置位，任何后续重放的成功都记 `DUPLICATE_EFFECT_SUPPRESSED`。
- **重试上限继续生效，人工重投不送自动重试额度**：每次重放只给**一次**受控投递机会，
  且 `attemptNo` 在历史计数上继续累加。原 3 次耗尽后重放的第 4 次投递若再失败，
  `attempts(4) >= maxAttempts(3)`，立即再次死信——不会因为人工介入白送新的退避重试轮次，
  更不会无限重投。想再试，需要再一次显式人工重投（第 2 次，`replayNo=2`，仍只给一次）。
- **水线协同**：死信位点若已在已提交水线之前，接受重投时水线同步退回到该位点前一位
  （运维操作是“只进不退”水线规则的**唯一**例外，轨迹编码仍记 `OFFSET_COMMITTED` 并写明
  是 operator-rewound）；消息重新到达终态后水线再次连续前进。
- **拒绝（可区分、不留半完成状态）**：
  - `REPLAY_REJECTED_NOT_DEAD`：目标位点存在但消息不是 `DEAD`（并发重放时，落选方会收到这个）；
  - `REPLAY_REJECTED_OFFSET_MISSING`：位点不存在（越界）；
  - 拒绝时不改任何状态、不动水线、不写重放记录、不落盘。
- 只作用于目标组，其他组的死信、位点、生效状态完全不受影响。

## 5b. 消费进度回退重放（Offset Rewind & Replay）

用于排查问题 / 重算结果：把某个组的已提交位点有控制地回退到更早位置，自该位点起的历史消息
（含以前成功过的）作为**重放**重新投递。

```java
// 水线当前为 29，回退到 20：消息 20..29 重新可投递；0..19 不动
List<ReplayRecord> reopened =
        broker.rewindCommittedOffset("order-group", 20, "ops-bob", "重算 20 之后的优惠结果");

// 处理器侧的标准幂等写法（broker 提供生效护栏，落盘的 succeededOnce 跨重启有效）
ConsumeOutcome handle(Message msg) {
    long offset = /* 从消息或上下文取位点 */;
    if (broker.hasBusinessEffectTaken(group, offset)) {
        return ConsumeOutcome.success();   // 以前已生效：跳过业务写，只让 broker 留“抑制”轨迹
    }
    // ...执行业务...
    return ConsumeOutcome.success();
}
```

语义与边界：

- **是重放，不是新消息**：重新投递的租约 `deliveryKind=OFFSET_REWIND`、`replayNo>=1`、
  `attemptNo` 延续历史（原首次为 1，重放即为 2）；操作本身记 `OFFSET_REWIND_ACCEPTED`
  （含回退前后位点、重新开放的成功/死信消息计数、operator、reason），每次投递记
  `REPLAY_DELIVERED`。
- **业务效果每组每条消息最多一次**：以前成功过的消息处理器仍会被调用（at-least-once 重放），
  但应先查 `hasBusinessEffectTaken` 跳过业务写；第二次成功 broker 记
  `DUPLICATE_EFFECT_SUPPRESSED`，`succeededOnce` 不翻转、终态仍是一次 `SUCCEEDED`。
  重放中再次失败仍按正常退避/上限规则处理；死信后也可再走人工死信重投。
- **只影响目标组**：其他组位点与状态一行不动。
- **回退点之前不动**：`< targetOffset` 的消息保持原状态与终态；窗口内 `PENDING` 保持待投、
  `RETRY_WAIT` 取消退避立即可投（不记为重放动作），终态消息逐笔写 `ReplayRecord` 后重新开放。
- **非法请求明确拒绝，原因可区分，水线保持原样**（均抛 `ReplayRejectedException`，携带编码与
  判定依据；校验全部在任何状态变更之前完成，失败不落盘、没有做了一半的进度）：
  - `REWIND_REJECTED_RANGE`：`target < 0` 或超过最大位点；
  - `REWIND_REJECTED_NOT_BEHIND`：target 不早于 `committed+1`（没有更早的位置可退；
    水线 -1 时回退 0 也落在这里）；
  - `REWIND_REJECTED_INFLIGHT`：从 target 起的任何位置（含水线之后已租出的消息）存在
    `IN_FLIGHT` 未确认租约——回退后水位重新前进时必然要越过这道坎，因此拒绝。
- 回退接受后 `committedOffset = targetOffset - 1`，随后由 broker 的“连续终态水位”规则重新推进。

## 5c. 并发与重启下的一致性

- 入队、poll、ack/nack、显式提交、**死信重投、进度回退**全部在同一把 broker 锁内“校验+变更+
  落盘”原子完成；运维操作允许在同组消费者并发运行期间执行。
- 并发期回退只要存在未确认租约必被 `REWIND_REJECTED_INFLIGHT` 拒绝；并发对同一条死信发起重放时，
  只有一个请求成功，其余收到 `REPLAY_REJECTED_NOT_DEAD`，不会重复发放租约。
- 落盘仍是“整状态写 `.tmp` + 原子 rename”；`ReplayRecord`、`succeededOnce`、`activeReplayNo/Kind`
  都在快照中，重放轮次跨重启可辨认、可继续审计。
- 重启回收对重放轮次内的租约一视同仁地计入尝试额度（恢复轨迹的尝试记录带原 `deliveryKind`），
  已耗尽额度的重放消息重启后直接死信，不会被无限重投。

## 6. 配置项（`application.properties`，前缀 `localmq`）

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
> **本轮没有新增配置项**：死信重投与进度回退都是显式 API 调用（人工运维动作，不走自动调度），
> 重试上限、租约、退避全部沿用目标组已有配置。

## 7. 使用示例

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

// 5) 人工死信重投（同一消息延续；每次只给一次受控投递，仍受 maxAttempts 约束）
broker.replayDeadLetter(group, offset, "ops-name", "人工补偿原因");

// 6) 消费进度回退重放（只影响目标组；有未确认租约时拒绝）
broker.rewindCommittedOffset(group, targetOffset, "ops-name", "重算/排查原因");
broker.hasBusinessEffectTaken(group, offset); // 处理器幂等护栏（跨重启有效）
broker.getReplays(group, offset);             // 某条消息的全部重放审计
broker.getAllReplays(group);                  // 本组所有重放审计
```

## 8. 测试覆盖（结论可复现）

| 测试类 | 场景 | 关键断言 |
|---|---|---|
| `DuplicateDeliveryTest` | 重复入队、成功后重复消费、迟到旧租约 nack | 业务只生效 1 次；三种忽略编码可区分 |
| `RetryAndDeadLetterTest` | 可重试失败耗尽、不可重试即死信（固定时钟+零退避） | 尝试次数精确、`cause` 可区分、原始消息保留、位点越过死信 |
| `ConcurrentConsumeOffsetTest` | 4 worker × 60 消息并发；回退/缺口/越界提交 | 每条生效 1 次、位点恰好到末端；三类拒绝编码分别正确 |
| `RestartRecoveryTest` | 成功/未确认/待处理三种消息跨“宕机”重启 | 未确认重投不重复生效、保留 `RECOVERED_AFTER_RESTART_UNACKED` 轨迹 |
| `DeadLetterReplayTest` | 死信重投成功 / 重放后再失败 / 两类拒绝 / 组间隔离 | attemptNo 延续、原死信不删、每轮只给 1 次且再失败立即死信、operator/replayNo 可区分 |
| `OffsetRewindReplayTest` | 回退重放成功消息、三类非法回退、组间隔离、重放后死信 | 重放标记 OFFSET_REWIND、业务生效仍 1 次（抑制轨迹）、拒绝原因分别正确、水线原样 |
| `ReplayConcurrencyTest` | 消费进行中回退、8 线程并发重投同一条死信、消费中回退重放 | INFLIGHT 拒绝且水线不动、恰 1 个重放胜出且无重复发放、24/40 条消息仍各生效 1 次 |
| `ReplayRestartBudgetTest` | 额度已耗尽时宕机、有余额时宕机、重放轮次内宕机 | 重启不增投递直接死信/正常重投计数延续、重放跨重启仍受限、原死信与重放死信各一条 |

测试日志通过标准输出的【输入】【位点】【判定】【依据】段落 + `MQ-TRACE` 日志打印**输入、位点与
判定依据**（含 operator、replayNo、attemptNo、拒绝原因与拒绝前后位点）。

### 本地验证（含“反复跑结果一致”的确认方法）

```bash
# 1) 连跑两遍必须都是 BUILD SUCCESS、24 个用例全绿
mvn test && mvn test

# 2) 更严格：clean 之后连跑两遍，覆盖“上一遍 target/ 整个被删掉”的冷启动
mvn clean test && mvn test
```

可重复运行的实现约定：

- 除两个 Spring 冒烟测试外，所有测试的状态文件都放在 JUnit 5 的 `@TempDir` 中，每次运行都是
  全新目录，天然不串状态；
- `AutoconfigureSmokeTest` 使用固定路径 `target/smoke-state.json`，在**静态初始化块**中删除残留
  （早于 Spring 上下文初始化与 broker 加载快照），因此上一遍留下的消息不会让第二遍的位点断言
  失败；`ApplicationTests` 的状态文件同样指向 `target/` 下的独立文件，不污染工作区 `data/`；
- 涉及时间的用例使用“每次读取固定步进”的 `TickClock` 或足够长的租约，避免时钟抖动影响结论。

