package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.GroupSnapshot;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地消息中间件模拟实现，承担四类职责：
 * <ol>
 *   <li>入队与按消费者组分发（不同组位点与生效状态互相独立）；</li>
 *   <li>幂等去重：全局重复 id 入队忽略；组内已 SUCCEEDED 的消息不再投递、不重复生效；</li>
 *   <li>租约 + 退避重试 + 死信，每次尝试留痕，原始记录全程保留；</li>
 *   <li>连续位点提交：只能提交到“连续终态水位”，回退与跨越未确认消息一律拒绝。</li>
 * </ol>
 *
 * <p>所有状态变更在锁内完成并立即通过 {@link BrokerStore} 持久化；所有判定都写
 * {@link TraceEvent}（同时打日志），作为可解释、可断言的依据。</p>
 */
public class LocalMessageBroker {

    private static final Logger log = LoggerFactory.getLogger(LocalMessageBroker.class);

    private final BrokerStore store;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final BrokerStore.BrokerState state;
    private final Map<String, ConsumerGroupConfig> configs = new ConcurrentHashMap<>();
    private final List<TraceEvent> trace = new CopyOnWriteArrayList<>();
    private long traceSeq;

    public LocalMessageBroker(BrokerStore store) {
        this(store, Clock.systemUTC());
    }

    public LocalMessageBroker(BrokerStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
        this.state = store.load();
        recover();
    }

    private long now() {
        return clock.millis();
    }

    // ------------------------------------------------------------------ 注册 / 恢复

    /**
     * 注册（重启后为重新关联）消费者组配置。组快照已存在时沿用磁盘上的位点与轨迹。
     */
    public void registerGroup(ConsumerGroupConfig config) {
        lock.lock();
        try {
            ConsumerGroupConfig old = configs.put(config.group(), config);
            if (old != null && !old.topic().equals(config.topic())) {
                throw new IllegalArgumentException("group '" + config.group()
                        + "' already registered with topic '" + old.topic()
                        + "', cannot rebind to '" + config.topic() + "'");
            }
            state.getGroups().computeIfAbsent(config.group(), g -> new GroupSnapshot(g));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重启恢复：将快照中仍为 IN_FLIGHT 的租约视为“进程死亡导致未确认”，
     * 追加 LEASE_TIMEOUT 轨迹后重置为 PENDING，立即可被重新投递；不改变任何终态。
     */
    private void recover() {
        lock.lock();
        try {
            long t = now();
            for (GroupSnapshot snap : state.getGroups().values()) {
                int recovered = 0;
                for (GroupSnapshot.Entry e : snap.getEntries()) {
                    if (e.state == MessageState.IN_FLIGHT) {
                        AttemptRecord rec = new AttemptRecord(
                                e.attemptCount,
                                e.currentConsumerId,
                                e.leaseExpiresAt > 0 ? e.leaseExpiresAt : t,
                                t,
                                AttemptResult.LEASE_TIMEOUT,
                                "RECOVERED_AFTER_RESTART_UNACKED",
                                "broker restarted before lease=" + e.currentLeaseId
                                        + " was acknowledged; message is redeliverable");
                        e.attempts.add(rec);
                        e.state = MessageState.PENDING;
                        e.currentLeaseId = null;
                        e.currentConsumerId = null;
                        e.leaseExpiresAt = 0;
                        e.nextVisibleAt = t;
                        recovered++;
                    }
                }
                if (recovered > 0) {
                    emit(snap.getGroup(), null, -1, DecisionCode.RECOVERED_ON_STARTUP,
                            recovered + " in-flight lease(s) reset to PENDING and will be redelivered; "
                                    + "committedOffset=" + snap.getCommittedOffset()
                                    + ", terminal states untouched");
                }
            }
            if (state.getMessages().size() > 0) {
                store.persist(state);
            }
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ 入队

    /**
     * 消息入队，并追加到所有已注册、订阅同主题的消费者组。
     * 相同 messageId 的重复入队被忽略，原因 {@link DecisionCode#ENQUEUE_DUPLICATED_GLOBAL_ID}。
     */
    public EnqueueResult enqueue(Message message) {
        lock.lock();
        try {
            long t = now();
            Message existing = state.getMessages().get(message.id());
            if (existing != null) {
                emit(null, message.id(), -1, DecisionCode.ENQUEUE_DUPLICATED_GLOBAL_ID,
                        "duplicate enqueue ignored: messageId=" + message.id()
                                + " first enqueuedAt=" + existing.enqueueAt()
                                + " incoming enqueueAt=" + message.enqueueAt());
                return new EnqueueResult(false, DecisionCode.ENQUEUE_DUPLICATED_GLOBAL_ID, existing);
            }
            state.getMessages().put(message.id(), message);
            for (ConsumerGroupConfig cfg : configs.values()) {
                if (cfg.topic().equals(message.topic())) {
                    GroupSnapshot snap = state.getGroups().get(cfg.group());
                    long offset = snap.getEntries().size();
                    snap.getEntries().add(new GroupSnapshot.Entry(offset, message.id()));
                }
            }
            emit(null, message.id(), -1, DecisionCode.ENQUEUED,
                    "message enqueued: topic='" + message.topic() + "' payload='" + message.payload()
                            + "' at=" + t);
            store.persist(state);
            return new EnqueueResult(true, DecisionCode.ENQUEUED, message);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ 投递

    /**
     * 请求一条消息（非阻塞，无消息返回 null）。投递前先回收超时租约；
     * 已提交位点之前、终态、有效租约期内、退避窗口内的消息都会被跳过并给出原因。
     */
    public LeasedMessage poll(String group, String consumerId) {
        lock.lock();
        try {
            ConsumerGroupConfig cfg = requireGroup(group);
            GroupSnapshot snap = state.getGroups().get(group);
            long t = now();
            reclaimExpiredLeases(cfg, snap, t);

            for (GroupSnapshot.Entry e : snap.getEntries()) {
                // 注意：不再按 committedOffset 跳过——人工重投的死信位于已提交位点之前，
                // 但处于 PENDING，必须能被再次投递；终态判断已足够挡住正常重复投递。
                if (e.state.isTerminal()) {
                    continue;
                }
                if (e.state == MessageState.IN_FLIGHT) {
                    continue; // reclaim 已处理过期租约，能走到这里说明租约仍有效
                }
                if (e.state == MessageState.RETRY_WAIT && e.nextVisibleAt > t) {
                    continue; // 退避中
                }
                if (e.attemptCount >= effectiveMaxAttempts(cfg, e)) {
                    // 尝试额度已耗尽（典型场景：重启前最后一次尝试未确认）。
                    // 重启不会多出投递机会：直接进死信，绝不发放第 maxAttempts+1 次租约。
                    moveToDead(snap, e, cfg, "MAX_ATTEMPTS_REACHED", t);
                    store.persist(state);
                    continue;
                }
                return grantLease(cfg, snap, e, consumerId, t);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void reclaimExpiredLeases(ConsumerGroupConfig cfg, GroupSnapshot snap, long t) {
        for (GroupSnapshot.Entry e : snap.getEntries()) {
            if (e.state == MessageState.IN_FLIGHT && e.leaseExpiresAt <= t) {
                String oldLease = e.currentLeaseId;
                AttemptRecord rec = new AttemptRecord(
                        e.attemptCount, e.currentConsumerId, e.leaseExpiresAt, t,
                        AttemptResult.LEASE_TIMEOUT, "LEASE_TIMEOUT_REDELIVER",
                        "lease=" + oldLease + " expired at " + e.leaseExpiresAt
                                + " without ack/nack; message becomes redeliverable");
                e.attempts.add(rec);
                e.state = MessageState.PENDING;
                e.currentLeaseId = null;
                e.currentConsumerId = null;
                e.leaseExpiresAt = 0;
                e.nextVisibleAt = t;
                emit(groupName(cfg), e.messageId, e.offset, DecisionCode.LEASE_TIMED_OUT,
                        "lease timed out: leaseId=" + oldLease + " attemptNo=" + e.attemptCount);
                if (e.attemptCount >= effectiveMaxAttempts(cfg, e)) {
                    moveToDead(snap, e, cfg, "MAX_ATTEMPTS_REACHED", t);
                    store.persist(state);
                }
            }
        }
    }

    /** 有效尝试上限：配置上限 + 人工重投次数（每次重投恰好多给一次机会）。 */
    private static int effectiveMaxAttempts(ConsumerGroupConfig cfg, GroupSnapshot.Entry e) {
        return cfg.maxAttempts() + e.redeliveryCount;
    }

    private LeasedMessage grantLease(ConsumerGroupConfig cfg, GroupSnapshot snap,
                                     GroupSnapshot.Entry e, String consumerId, long t) {
        Message message = state.getMessages().get(e.messageId);
        e.attemptCount++;
        String leaseId = UUID.randomUUID().toString();
        long expires = t + cfg.leaseDuration().toMillis();
        e.state = MessageState.IN_FLIGHT;
        e.currentLeaseId = leaseId;
        e.currentConsumerId = consumerId;
        e.leaseExpiresAt = expires;
        // 投递触发原因：人工重投 / 进度重放 / 普通（首次或自动重试），取自从 PENDING 恢复时
        // 留下的持久化标记，发放租约后即消费掉。
        String cause = e.pendingCause == null ? LeasedMessage.CAUSE_NORMAL : e.pendingCause;
        e.pendingCause = null;
        LeasedMessage lease = new LeasedMessage(leaseId, cfg.group(), e.offset, message,
                e.attemptCount, t, expires, cause);
        DecisionCode code = switch (cause) {
            case LeasedMessage.CAUSE_REDELIVER -> DecisionCode.REDELIVER_DISPATCHED;
            case LeasedMessage.CAUSE_REPLAY -> DecisionCode.REPLAY_DISPATCHED;
            default -> DecisionCode.DISPATCHED;
        };
        String origin = switch (cause) {
            case LeasedMessage.CAUSE_REDELIVER ->
                    "manual redelivery #" + e.redeliveryCount + " from dead-letter";
            case LeasedMessage.CAUSE_REPLAY ->
                    "replay #" + e.replayCount + " after offset rewind";
            default -> e.attemptCount == 1 ? "PENDING(first)" : "retry";
        };
        emit(groupName(cfg), e.messageId, e.offset, code,
                "dispatched: consumer=" + consumerId + " attemptNo=" + e.attemptCount
                        + " deliveryCause=" + cause + " leaseExpiresAt=" + expires
                        + " origin=" + origin);
        store.persist(state);
        return lease;
    }

    // ------------------------------------------------------------------ 确认

    /** 确认成功：组内同一条消息只可能走到这里一次（终态与租约双重防护）。 */
    public void acknowledgeSuccess(LeasedMessage lease) {
        complete(lease, ConsumeOutcome.success(), null);
    }

    /**
     * 确认失败。
     *
     * @param retryable true 按退避重试（耗尽后死信）；false 立即死信
     * @param reason    失败原因（异常类型或业务说明）
     */
    public void acknowledgeFailure(LeasedMessage lease, boolean retryable, String reason) {
        complete(lease,
                retryable ? ConsumeOutcome.retry(reason) : ConsumeOutcome.dead(reason),
                null);
    }

    /** 通用确认入口，outcome 为处理器结论，thrown 为处理器抛出的异常（可为 null）。 */
    public void complete(LeasedMessage lease, ConsumeOutcome outcome, Throwable thrown) {
        lock.lock();
        try {
            long t = now();
            ConsumerGroupConfig cfg = requireGroup(lease.group());
            GroupSnapshot snap = state.getGroups().get(lease.group());
            GroupSnapshot.Entry e = snap.entryAt(lease.offset());
            if (e == null) {
                emit(lease.group(), lease.message().id(), lease.offset(),
                        DecisionCode.LEASE_STALE_IGNORED,
                        "ack ignored: offset does not exist, leaseId=" + lease.leaseId());
                return;
            }
            if (!lease.leaseId().equals(e.currentLeaseId) || e.state != MessageState.IN_FLIGHT) {
                emit(lease.group(), e.messageId, e.offset, DecisionCode.LEASE_STALE_IGNORED,
                        "ack ignored: stale or superseded lease, incoming=" + lease.leaseId()
                                + " current=" + e.currentLeaseId + " state=" + e.state
                                + " (business effect of this ack is discarded)");
                return;
            }

            String detail = thrown != null
                    ? outcome.reason() + ": " + thrown
                    : outcome.reason();
            AttemptResult result = switch (outcome.verdict()) {
                case SUCCESS -> AttemptResult.SUCCESS;
                case RETRY -> AttemptResult.RETRYABLE_FAILURE;
                case DEAD -> AttemptResult.NON_RETRYABLE_FAILURE;
            };
            AttemptRecord rec = new AttemptRecord(
                    lease.attemptNo(), e.currentConsumerId,
                    lease.grantedAt(), t, result, outcome.reason(), detail);
            e.attempts.add(rec);
            e.currentLeaseId = null;
            e.leaseExpiresAt = 0;

            switch (outcome.verdict()) {
                case SUCCESS -> {
                    e.state = MessageState.SUCCEEDED;
                    e.currentConsumerId = null;
                    emit(lease.group(), e.messageId, e.offset, DecisionCode.ACK_SUCCEEDED,
                            "success acknowledged, business effect committed once: attemptNo="
                                    + lease.attemptNo() + " reason=" + outcome.reason());
                    advanceCommittedOffset(cfg, snap, t);
                }
                case RETRY -> {
                    int effectiveMax = effectiveMaxAttempts(cfg, e);
                    if (e.attempts.size() >= effectiveMax) {
                        moveToDead(snap, e, cfg, "MAX_ATTEMPTS_REACHED", t);
                    } else {
                        long backoff = cfg.backoffPolicy().nextBackoffMillis(e.attempts.size());
                        e.state = MessageState.RETRY_WAIT;
                        e.currentConsumerId = null;
                        e.nextVisibleAt = t + backoff;
                        emit(lease.group(), e.messageId, e.offset, DecisionCode.NACK_RETRY_SCHEDULED,
                                "retryable failure: attemptNo=" + e.attempts.size()
                                        + "/" + effectiveMax + " backoffMs=" + backoff
                                        + " nextVisibleAt=" + e.nextVisibleAt + " reason=" + detail);
                    }
                }
                case DEAD -> moveToDead(snap, e, cfg, "NON_RETRYABLE_BY_HANDLER", t);
            }
            store.persist(state);
        } finally {
            lock.unlock();
        }
    }

    private void moveToDead(GroupSnapshot snap, GroupSnapshot.Entry e,
                            ConsumerGroupConfig cfg, String cause, long t) {
        e.state = MessageState.DEAD;
        e.currentConsumerId = null;
        e.nextVisibleAt = 0;
        Message message = state.getMessages().get(e.messageId);
        DeadLetter dl = new DeadLetter(message, cfg.group(), new ArrayList<>(e.attempts), t, cause);
        state.getDeadLetters().add(dl);
        DecisionCode code = "NON_RETRYABLE_BY_HANDLER".equals(cause)
                ? DecisionCode.NACK_NON_RETRYABLE
                : DecisionCode.NACK_RETRY_EXHAUSTED;
        emit(cfg.group(), e.messageId, e.offset, code,
                "moved to dead-letter: cause=" + cause + " attempts=" + e.attempts.size()
                        + " maxAttempts=" + cfg.maxAttempts() + " (original message and all "
                        + "attempt records retained)");
        advanceCommittedOffset(cfg, snap, t);
    }

    /** 把连续终态水位向前推进（仅前进，绝不回退）。 */
    private void advanceCommittedOffset(ConsumerGroupConfig cfg, GroupSnapshot snap, long t) {
        long next = snap.getCommittedOffset();
        while (true) {
            GroupSnapshot.Entry e = snap.entryAt(next + 1);
            if (e == null || !e.state.isTerminal()) {
                break;
            }
            next++;
        }
        if (next > snap.getCommittedOffset()) {
            long old = snap.getCommittedOffset();
            snap.setCommittedOffset(next);
            emit(cfg.group(), null, next, DecisionCode.OFFSET_COMMITTED,
                    "committed offset auto-advanced " + old + " -> " + next
                            + " (all messages up to this offset are terminal)");
        }
    }

    // ------------------------------------------------------------------ 位点提交

    /**
     * 显式提交位点：语义为“offset 及其之前的消息均已确认”。
     * 回退、跨越未确认消息、超出范围一律抛 {@link OffsetCommitRejectedException}。
     */
    public void commitOffset(String group, long offset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot snap = state.getGroups().get(group);
            long current = snap.getCommittedOffset();
            long maxOffset = snap.getEntries().size() - 1L;

            if (offset < current) {
                reject(group, offset, current, DecisionCode.OFFSET_COMMIT_REJECTED_ROLLBACK,
                        "rollback commit forbidden: requested " + offset + " < committed " + current);
            }
            if (offset > maxOffset) {
                reject(group, offset, current, DecisionCode.OFFSET_COMMIT_REJECTED_RANGE,
                        "offset out of range: requested " + offset + " > max " + maxOffset);
            }
            for (long o = current + 1; o <= offset; o++) {
                GroupSnapshot.Entry e = snap.entryAt(o);
                if (e == null || !e.state.isTerminal()) {
                    reject(group, offset, current, DecisionCode.OFFSET_COMMIT_REJECTED_GAP,
                            "cannot commit past unacknowledged message: offset=" + o
                                    + " is " + (e == null ? "MISSING" : e.state)
                                    + " (committed=" + current + ")");
                }
            }
            if (offset > current) {
                snap.setCommittedOffset(offset);
                emit(group, null, offset, DecisionCode.OFFSET_COMMITTED,
                        "explicit commit accepted: committed " + current + " -> " + offset);
                store.persist(state);
            } else {
                emit(group, null, offset, DecisionCode.OFFSET_COMMITTED,
                        "explicit commit accepted (no movement): committed stays " + current);
            }
        } finally {
            lock.unlock();
        }
    }

    private void reject(String group, long requested, long current,
                        DecisionCode code, String detail) {
        emit(group, null, requested, code, detail);
        throw new OffsetCommitRejectedException(group, requested, current, code, detail);
    }

    // ------------------------------------------------------------------ 消费进度重放

    /**
     * 把目标消费者组的消费进度回退到 {@code targetOffset}，此后已处理过的消息会被重新投递，
     * 用于排查问题或重算结果。
     *
     * <p>语义：</p>
     * <ul>
     *   <li>只影响目标组，其他组的位点与状态不变；</li>
     *   <li>仅允许回退（target &lt; 当前已提交位点）；区间内 SUCCEEDED 的消息被重置为待重放，
     *       重放投递带 {@link DecisionCode#REPLAY_DISPATCHED} 轨迹与 {@code deliveryCause=REPLAY}，
     *       业务侧凭 messageId 幂等，生效仍最多一次；</li>
     *   <li>区间内 IN_FLIGHT 的消息不被重置——不越过任何未确认消息，进行中的确认照常生效，
     *       不与本次回退互相覆盖；DEAD 消息保持死信（请用死信重投）；</li>
     *   <li>校验全部通过后才改动状态，且在同一把锁内一次持久化：被拒绝时不留半截进度。</li>
     * </ul>
     *
     * @throws ReplayRejectedException 目标越界或不是回退（原因可区分，状态不变）
     */
    public ReplayResult replayFrom(String group, long targetOffset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot snap = state.getGroups().get(group);
            long current = snap.getCommittedOffset();
            long maxOffset = snap.getEntries().size() - 1L;

            if (targetOffset < -1 || targetOffset > maxOffset) {
                emit(group, null, targetOffset, DecisionCode.REPLAY_REJECTED_RANGE,
                        "replay rejected: target " + targetOffset + " out of range [-1, "
                                + maxOffset + "] (committed stays " + current + ")");
                throw new ReplayRejectedException(group, targetOffset, current,
                        DecisionCode.REPLAY_REJECTED_RANGE,
                        "target offset out of range [-1, " + maxOffset + "]");
            }
            if (targetOffset >= current) {
                emit(group, null, targetOffset, DecisionCode.REPLAY_REJECTED_NOT_BACKWARD,
                        "replay rejected: target " + targetOffset + " is not a rewind of "
                                + "committed " + current + " (replay only moves backwards; "
                                + "committed stays " + current + ")");
                throw new ReplayRejectedException(group, targetOffset, current,
                        DecisionCode.REPLAY_REJECTED_NOT_BACKWARD,
                        "target offset must be < current committed offset");
            }

            long t = now();
            int reset = 0, inFlight = 0, dead = 0;
            for (long o = targetOffset + 1; o <= current; o++) {
                GroupSnapshot.Entry e = snap.entryAt(o);
                if (e == null) {
                    continue;
                }
                switch (e.state) {
                    case SUCCEEDED -> {
                        e.state = MessageState.PENDING;
                        e.currentLeaseId = null;
                        e.currentConsumerId = null;
                        e.leaseExpiresAt = 0;
                        e.nextVisibleAt = t;
                        e.replayCount++;
                        e.pendingCause = LeasedMessage.CAUSE_REPLAY;
                        reset++;
                    }
                    case IN_FLIGHT -> inFlight++;
                    case DEAD -> dead++;
                    default -> { /* PENDING/RETRY_WAIT 本就待投递，无需重置 */ }
                }
            }
            snap.setCommittedOffset(targetOffset);
            emit(group, null, targetOffset, DecisionCode.REPLAY_SCHEDULED,
                    "replay scheduled: committed " + current + " -> " + targetOffset
                            + ", resetToPending=" + reset + " (SUCCEEDED, will be redelivered "
                            + "with deliveryCause=REPLAY), leftInFlight=" + inFlight
                            + " (ongoing acks untouched), leftDead=" + dead
                            + " (use dead-letter redelivery for those); other groups unaffected");
            store.persist(state);
            return new ReplayResult(group, current, targetOffset, reset, inFlight, dead);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ 死信重投（人工补偿）

    /**
     * 把已进入死信的消息人工重投回本消费者组，用于人工补偿。
     *
     * <p>语义：</p>
     * <ul>
     *   <li>是同一条消息的延续：offset、messageId、全部尝试轨迹与 attemptCount 原样保留，
     *       重投后的投递 attemptNo 继续递增，业务生效仍全组最多一次；</li>
     *   <li>原 {@link DeadLetter} 记录不删除、不修改；若重投后再次失败耗尽，会追加一条新的死信记录；</li>
     *   <li>每次重投在 {@code maxAttempts} 之外恰好多给一次投递机会（有效上限 =
     *       maxAttempts + redeliveryCount），重投后的失败仍受重试上限约束，不会无限重试；</li>
     *   <li>与自动重试可区分：轨迹含 {@link DecisionCode#REDELIVERED_FROM_DEAD_LETTER} 与
     *       {@link DecisionCode#REDELIVER_DISPATCHED}，租约上 {@code deliveryCause=REDELIVER}。</li>
     * </ul>
     *
     * @return 被重投消息在组内的位点
     * @throws RedeliverRejectedException 消息不存在或当前不是死信终态（原因可区分，状态不变）
     */
    public long redeliverDeadLetter(String group, String messageId) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot snap = state.getGroups().get(group);
            GroupSnapshot.Entry target = null;
            for (GroupSnapshot.Entry e : snap.getEntries()) {
                if (e.messageId.equals(messageId)) {
                    target = e;
                    break;
                }
            }
            if (target == null) {
                emit(group, messageId, -1, DecisionCode.REDELIVER_REJECTED_UNKNOWN_MESSAGE,
                        "redelivery rejected: no message with id=" + messageId
                                + " in group='" + group + "' (state unchanged)");
                throw new RedeliverRejectedException(group, messageId,
                        DecisionCode.REDELIVER_REJECTED_UNKNOWN_MESSAGE,
                        "message not found in this consumer group");
            }
            if (target.state != MessageState.DEAD) {
                emit(group, messageId, target.offset, DecisionCode.REDELIVER_REJECTED_NOT_DEAD,
                        "redelivery rejected: message state=" + target.state
                                + " (only DEAD terminal messages can be redelivered; "
                                + "state and offset unchanged)");
                throw new RedeliverRejectedException(group, messageId,
                        DecisionCode.REDELIVER_REJECTED_NOT_DEAD,
                        "message state is " + target.state + ", not DEAD");
            }
            long t = now();
            target.state = MessageState.PENDING;
            target.currentLeaseId = null;
            target.currentConsumerId = null;
            target.leaseExpiresAt = 0;
            target.nextVisibleAt = t;
            target.redeliveryCount++;
            target.pendingCause = LeasedMessage.CAUSE_REDELIVER;
            emit(group, messageId, target.offset, DecisionCode.REDELIVERED_FROM_DEAD_LETTER,
                    "dead-letter manually redelivered: redeliveryNo=" + target.redeliveryCount
                            + " attemptsSoFar=" + target.attemptCount
                            + " effectiveMaxAttempts=" + effectiveMaxAttempts(
                                    configs.get(group), target)
                            + " (original dead-letter record retained; this is a continuation "
                            + "of the same message, not a new one)");
            store.persist(state);
            return target.offset;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ 查询

    public long getCommittedOffset(String group) {
        lock.lock();
        try {
            requireGroup(group);
            return state.getGroups().get(group).getCommittedOffset();
        } finally {
            lock.unlock();
        }
    }

    public MessageState getMessageState(String group, long offset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot.Entry e = state.getGroups().get(group).entryAt(offset);
            return e == null ? null : e.state;
        } finally {
            lock.unlock();
        }
    }

    public List<AttemptRecord> getAttempts(String group, long offset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot.Entry e = state.getGroups().get(group).entryAt(offset);
            return e == null ? List.of() : new ArrayList<>(e.attempts);
        } finally {
            lock.unlock();
        }
    }

    public int getAttemptCount(String group, long offset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot.Entry e = state.getGroups().get(group).entryAt(offset);
            return e == null ? 0 : e.attemptCount;
        } finally {
            lock.unlock();
        }
    }

    public List<DeadLetter> getDeadLetters() {
        lock.lock();
        try {
            return new ArrayList<>(state.getDeadLetters());
        } finally {
            lock.unlock();
        }
    }

    /** 全部审计轨迹（按产生顺序）。 */
    public List<TraceEvent> getTrace() {
        return new ArrayList<>(trace);
    }

    /** 最近一条与给定消息相关的轨迹，便于测试精确断言忽略/失败原因。 */
    public TraceEvent lastTraceFor(String group, String messageId) {
        TraceEvent found = null;
        for (TraceEvent ev : trace) {
            boolean groupOk = group == null || group.equals(ev.group());
            if (groupOk && messageId != null && messageId.equals(ev.messageId())) {
                found = ev;
            }
        }
        return found;
    }

    /** 强制持久化（测试与优雅停机时可用）。 */
    public void flush() {
        lock.lock();
        try {
            store.persist(state);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ 内部工具

    private ConsumerGroupConfig requireGroup(String group) {
        ConsumerGroupConfig cfg = configs.get(group);
        if (cfg == null) {
            throw new IllegalArgumentException("consumer group not registered: " + group);
        }
        return cfg;
    }

    private static String groupName(ConsumerGroupConfig cfg) {
        return cfg.group();
    }

    private static String consumerOf(GroupSnapshot.Entry e) {
        return e.currentConsumerId;
    }

    private TraceEvent emit(String group, String messageId, long offset,
                            DecisionCode code, String detail) {
        TraceEvent ev = new TraceEvent(++traceSeq, now(), group, messageId, offset, code, detail);
        trace.add(ev);
        log.info("[MQ-TRACE] seq={} group={} messageId={} offset={} code={} | {}",
                ev.seq(), ev.group(), ev.messageId(), ev.offset(), ev.code(), ev.detail());
        return ev;
    }
}
