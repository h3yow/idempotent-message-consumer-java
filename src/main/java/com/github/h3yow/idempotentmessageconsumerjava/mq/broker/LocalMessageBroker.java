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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryKind;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ReplayRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.ops.ReplayRejectedException;

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
    private final Set<String> recoveredGroups = new HashSet<>();
    private final List<TraceEvent> trace = new CopyOnWriteArrayList<>();
    private long traceSeq;

    public LocalMessageBroker(BrokerStore store) {
        this(store, Clock.systemUTC());
    }

    public LocalMessageBroker(BrokerStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
        this.state = store.load();
        // 兼容旧快照：补齐新增字段
        state.getGroups().values().forEach(GroupSnapshot::normalizeAfterLoad);
    }

    private long now() {
        return clock.millis();
    }

    // ------------------------------------------------------------------ 注册 / 恢复

    /**
     * 注册（重启后为重新关联）消费者组配置。组快照已存在时沿用磁盘上的位点与轨迹。
     * 重启后第一次注册某个组时，对该组执行一次“未确认租约恢复”。
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
            GroupSnapshot snap =
                    state.getGroups().computeIfAbsent(config.group(), g -> new GroupSnapshot(g));
            snap.normalizeAfterLoad();
            if (recoveredGroups.add(config.group())) {
                recoverGroup(config, snap);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 重启恢复（按组）：将快照中仍为 IN_FLIGHT 的租约视为“进程死亡导致未确认”，
     * 追加 LEASE_TIMEOUT 轨迹后重置为 PENDING，立即可被重新投递；不改变任何终态。
     *
     * <p>尝试额度账对齐：之前已消耗的尝试次数继续算数。回收占满最后一次尝试额度
     * （{@code attemptCount >= maxAttempts}）的消息不再给新的投递机会，直接进入死信，
     * 重启不能突破配置的重试上限、也不能把已耗尽的消息无限重投。</p>
     */
    private void recoverGroup(ConsumerGroupConfig cfg, GroupSnapshot snap) {
        long t = now();
        int recovered = 0;
        int exhausted = 0;
        boolean changed = false;
        for (GroupSnapshot.Entry e : snap.getEntries()) {
            if (e.state == MessageState.IN_FLIGHT) {
                String oldLease = e.currentLeaseId;
                AttemptRecord rec = new AttemptRecord(
                        e.attemptCount,
                        e.currentConsumerId,
                        e.leaseExpiresAt > 0 ? e.leaseExpiresAt : t,
                        t,
                        AttemptResult.LEASE_TIMEOUT,
                        "RECOVERED_AFTER_RESTART_UNACKED",
                        "broker restarted before lease=" + oldLease
                                + " was acknowledged; consumed attempt budget stays counted",
                        e.activeReplayKind == null ? DeliveryKind.NORMAL : e.activeReplayKind);
                e.attempts.add(rec);
                e.currentLeaseId = null;
                e.currentConsumerId = null;
                e.leaseExpiresAt = 0;
                e.nextVisibleAt = t;
                recovered++;
                changed = true;
                if (e.attemptCount >= cfg.maxAttempts()) {
                    // 额度已耗尽：不再给投递机会，老老实实进死信（重启不增加投递机会）
                    e.state = MessageState.PENDING;
                    moveToDead(snap, e, cfg, "MAX_ATTEMPTS_REACHED", t);
                    exhausted++;
                } else {
                    e.state = MessageState.PENDING;
                }
            }
        }
        if (recovered > 0) {
            emit(snap.getGroup(), null, -1, DecisionCode.RECOVERED_ON_STARTUP,
                    recovered + " in-flight lease(s) reset after restart; " + exhausted
                            + " of them had exhausted the attempt budget and went straight to "
                            + "dead-letter; committedOffset=" + snap.getCommittedOffset()
                            + ", terminal states untouched");
            store.persist(state);
        } else if (changed) {
            store.persist(state);
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
                if (e.offset <= snap.getCommittedOffset()) {
                    continue; // 已提交，协议上不可能再投递
                }
                if (e.state.isTerminal()) {
                    continue;
                }
                if (e.state == MessageState.IN_FLIGHT) {
                    continue; // reclaim 已处理过期租约，能走到这里说明租约仍有效
                }
                if (e.state == MessageState.RETRY_WAIT && e.nextVisibleAt > t) {
                    continue; // 退避中
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
                DeliveryKind kind = e.activeReplayKind == null
                        ? DeliveryKind.NORMAL : e.activeReplayKind;
                AttemptRecord rec = new AttemptRecord(
                        e.attemptCount, e.currentConsumerId, e.leaseExpiresAt, t,
                        AttemptResult.LEASE_TIMEOUT, "LEASE_TIMEOUT_REDELIVER",
                        "lease=" + oldLease + " expired at " + e.leaseExpiresAt
                                + " without ack/nack; message becomes redeliverable"
                                + (kind == DeliveryKind.NORMAL ? ""
                                        : " (still within replay round #" + e.activeReplayNo + ")"),
                        kind);
                e.attempts.add(rec);
                e.state = MessageState.PENDING;
                e.currentLeaseId = null;
                e.currentConsumerId = null;
                e.leaseExpiresAt = 0;
                e.nextVisibleAt = t;
                emit(groupName(cfg), e.messageId, e.offset, DecisionCode.LEASE_TIMED_OUT,
                        "lease timed out: leaseId=" + oldLease + " attemptNo=" + e.attemptCount
                                + " replayNo=" + e.activeReplayNo);
                if (e.attemptCount >= cfg.maxAttempts()) {
                    moveToDead(snap, e, cfg, "MAX_ATTEMPTS_REACHED", t);
                    store.persist(state);
                }
            }
        }
    }

    private LeasedMessage grantLease(ConsumerGroupConfig cfg, GroupSnapshot snap,
                                     GroupSnapshot.Entry e, String consumerId, long t) {
        Message message = state.getMessages().get(e.messageId);
        e.attemptCount++;
        String leaseId = UUID.randomUUID().toString();
        long expires = t + cfg.leaseDuration().toMillis();
        DeliveryKind kind = e.activeReplayKind == null ? DeliveryKind.NORMAL : e.activeReplayKind;
        e.state = MessageState.IN_FLIGHT;
        e.currentLeaseId = leaseId;
        e.currentConsumerId = consumerId;
        e.leaseExpiresAt = expires;
        LeasedMessage lease = new LeasedMessage(leaseId, cfg.group(), e.offset, message,
                e.attemptCount, t, expires, kind, e.activeReplayNo);
        DecisionCode code = switch (kind) {
            case DEAD_LETTER_REPLAY -> DecisionCode.REPLAY_DISPATCHED;
            case OFFSET_REWIND -> DecisionCode.REPLAY_DELIVERED;
            case NORMAL -> DecisionCode.DISPATCHED;
        };
        String kindTag = switch (kind) {
            case DEAD_LETTER_REPLAY -> "MANUAL_DEAD_LETTER_REPLAY#" + e.activeReplayNo;
            case OFFSET_REWIND -> "OFFSET_REWIND_REPLAY#" + e.activeReplayNo;
            case NORMAL -> e.attemptCount == 1 ? "PENDING(first)" : "retry";
        };
        emit(groupName(cfg), e.messageId, e.offset, code,
                "dispatched: consumer=" + consumerId + " attemptNo=" + e.attemptCount
                        + " leaseExpiresAt=" + expires + " kind=" + kindTag
                        + " (same message continuation, not a new message)");
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
                    lease.grantedAt(), t, result, outcome.reason(), detail,
                    lease.deliveryKind());
            e.attempts.add(rec);
            e.currentLeaseId = null;
            e.leaseExpiresAt = 0;

            switch (outcome.verdict()) {
                case SUCCESS -> {
                    e.state = MessageState.SUCCEEDED;
                    e.currentConsumerId = null;
                    boolean firstEffect = !e.succeededOnce;
                    e.succeededOnce = true;
                    int finishedReplayNo = e.activeReplayNo;
                    String finishedReplayOperator = e.activeReplayOperator;
                    clearActiveReplay(e);
                    if (firstEffect) {
                        emit(lease.group(), e.messageId, e.offset, DecisionCode.ACK_SUCCEEDED,
                                "success acknowledged, business effect committed once: attemptNo="
                                        + lease.attemptNo() + " kind=" + lease.deliveryKind()
                                        + " reason=" + outcome.reason());
                    } else {
                        // 回退重放以前成功过的消息：处理器确实又跑了一次，但业务效果幂等抑制，只留痕
                        emit(lease.group(), e.messageId, e.offset,
                                DecisionCode.DUPLICATE_EFFECT_SUPPRESSED,
                                "replayed message had already taken effect in this group; "
                                        + "handler ran again but business effect is suppressed once: "
                                        + "attemptNo=" + lease.attemptNo()
                                        + " replayNo=" + finishedReplayNo
                                        + " operator=" + finishedReplayOperator
                                        + " reason=" + outcome.reason());
                    }
                    advanceCommittedOffset(cfg, snap, t);
                }
                case RETRY -> {
                    if (e.attempts.size() >= cfg.maxAttempts()) {
                        moveToDead(snap, e, cfg, "MAX_ATTEMPTS_REACHED", t);
                    } else {
                        long backoff = cfg.backoffPolicy().nextBackoffMillis(e.attempts.size());
                        e.state = MessageState.RETRY_WAIT;
                        e.currentConsumerId = null;
                        e.nextVisibleAt = t + backoff;
                        emit(lease.group(), e.messageId, e.offset, DecisionCode.NACK_RETRY_SCHEDULED,
                                "retryable failure: attemptNo=" + e.attempts.size()
                                        + "/" + cfg.maxAttempts() + " backoffMs=" + backoff
                                        + " nextVisibleAt=" + e.nextVisibleAt + " replayNo="
                                        + e.activeReplayNo + " reason=" + detail);
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
        int replayNo = e.activeReplayNo;
        String operator = e.activeReplayOperator;
        clearActiveReplay(e);
        Message message = state.getMessages().get(e.messageId);
        DeadLetter dl = new DeadLetter(message, cfg.group(), new ArrayList<>(e.attempts), t,
                cause, replayNo, operator);
        state.getDeadLetters().add(dl);
        DecisionCode code = "NON_RETRYABLE_BY_HANDLER".equals(cause)
                ? DecisionCode.NACK_NON_RETRYABLE
                : DecisionCode.NACK_RETRY_EXHAUSTED;
        emit(cfg.group(), e.messageId, e.offset, code,
                "moved to dead-letter: cause=" + cause + " attempts=" + e.attempts.size()
                        + " maxAttempts=" + cfg.maxAttempts()
                        + (replayNo > 0 ? " replayNo=" + replayNo + " operator=" + operator
                        : " (original death, replayNo=0)")
                        + " (original message and all attempt records retained; "
                        + "previous dead-letter record is not erased)");
        advanceCommittedOffset(cfg, snap, t);
    }

    private static void clearActiveReplay(GroupSnapshot.Entry e) {
        e.activeReplayNo = 0;
        e.activeReplayKind = DeliveryKind.NORMAL;
        e.activeReplayOperator = null;
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

    // ------------------------------------------------------ 运维：死信重投 / 进度回退

    /** 人工死信重投（默认操作人）。 */
    public ReplayRecord replayDeadLetter(String group, long offset, String reason) {
        return replayDeadLetter(group, offset, "manual-operator", reason);
    }

    /**
     * 人工死信重投：把已进入死信的同一条消息作为<b>延续</b>（不是新消息）重新放回本组再处理。
     *
     * <p>语义与边界：</p>
     * <ul>
     *   <li>只有 {@code DEAD} 终态的消息可重投；其他状态以可区分编码拒绝，状态保持原样；</li>
     *   <li>原有死信记录一条都不删，审计轨迹明确记录操作人/原因/这是第几次重放；</li>
     *   <li>重放投递的 attemptNo 延续历史序号，重放只给“一次投递机会”：此后再失败仍受
     *       组配置的 {@code maxAttempts} 约束（自动退避重试的剩余额度=maxAttempts-attemptCount），
     *       不会因为人工介入而获得无限机会；</li>
     *   <li>若该位点在已提交水线之前，水线同步回退到该位点之前（运维操作是水线唯一允许的回退），
     *       回退不会越过任何更低位的未确认消息；</li>
     *   <li>与并发消费共存：全程持同一把锁，校验与变更原子完成，失败不留下半完成进度。</li>
     * </ul>
     */
    public ReplayRecord replayDeadLetter(String group, long offset,
                                         String operator, String reason) {
        lock.lock();
        try {
            ConsumerGroupConfig cfg = requireGroup(group);
            GroupSnapshot snap = state.getGroups().get(group);
            long current = snap.getCommittedOffset();
            GroupSnapshot.Entry e = snap.entryAt(offset);
            if (e == null) {
                rejectReplay(group, offset, current, DecisionCode.REPLAY_REJECTED_OFFSET_MISSING,
                        "dead-letter replay rejected: offset=" + offset + " does not exist"
                                + " (maxOffset=" + (snap.getEntries().size() - 1L) + ")");
            }
            assert e != null;
            if (e.state != MessageState.DEAD) {
                rejectReplay(group, offset, current, DecisionCode.REPLAY_REJECTED_NOT_DEAD,
                        "dead-letter replay rejected: offset=" + offset + " messageId="
                                + e.messageId + " is " + e.state
                                + ", only DEAD messages can be manually replayed");
            }
            long t = now();
            int replayNo = e.replayCount() + 1;
            ReplayRecord rec = new ReplayRecord(replayNo, DeliveryKind.DEAD_LETTER_REPLAY,
                    operator, reason, e.offset, e.messageId, t);
            e.replays.add(rec);
            // 同一消息的延续：attemptCount/attempts/死信历史全部保留，只重新开放投递
            e.state = MessageState.PENDING;
            e.currentLeaseId = null;
            e.currentConsumerId = null;
            e.leaseExpiresAt = 0;
            e.nextVisibleAt = t;
            e.activeReplayNo = replayNo;
            e.activeReplayKind = DeliveryKind.DEAD_LETTER_REPLAY;
            e.activeReplayOperator = operator;
            if (snap.getCommittedOffset() >= e.offset) {
                long old = snap.getCommittedOffset();
                snap.setCommittedOffset(e.offset - 1);
                emit(group, e.messageId, e.offset, DecisionCode.OFFSET_COMMITTED,
                        "committed offset operator-rewound for dead-letter replay: "
                                + old + " -> " + snap.getCommittedOffset()
                                + " (no unacknowledged message below is crossed)");
            }
            emit(group, e.messageId, e.offset, DecisionCode.DEAD_LETTER_REPLAY_ACCEPTED,
                    "manual dead-letter replay accepted: replayNo=" + replayNo
                            + " operator=" + operator + " reason=" + reason
                            + " historicalAttempts=" + e.attempts.size()
                            + " maxAttempts=" + cfg.maxAttempts()
                            + " (continuation of the same message; original dead-letter "
                            + "record retained; retry budget stays capped)");
            store.persist(state);
            return rec;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 消费进度回退重放：把本组的已提交位点有控制地回退到 {@code targetOffset}，
     * 自该位点起的历史消息（含以前成功过的）作为<b>重放</b>重新投递，而不是新消息。
     *
     * <p>只影响目标组，其他组的位点与状态完全不动。回退后：</p>
     * <ul>
     *   <li>区间内任何 {@code IN_FLIGHT}（未确认租约）都使操作以
     *       {@link DecisionCode#REWIND_REJECTED_INFLIGHT} 拒绝——不能越过还没确认的消息；</li>
     *   <li>target 不早于当前水线（没有更早的位置可退）以
     *       {@link DecisionCode#REWIND_REJECTED_NOT_BEHIND} 拒绝；</li>
     *   <li>target 越界以 {@link DecisionCode#REWIND_REJECTED_RANGE} 拒绝；</li>
     *   <li>三类拒绝互不混淆，且拒绝时不做任何变更、不落盘；</li>
     *   <li>以前成功过的消息重放时仍带历史 attemptNo 与 replayNo；其业务效果由
     *       {@link #hasBusinessEffectTaken(String, long)} 幂等查询配合处理器抑制，
     *       broker 对“第二次成功”记 {@link DecisionCode#DUPLICATE_EFFECT_SUPPRESSED}。</li>
     * </ul>
     */
    public List<ReplayRecord> rewindCommittedOffset(String group, long targetOffset,
                                                    String operator, String reason) {
        lock.lock();
        try {
            ConsumerGroupConfig cfg = requireGroup(group);
            GroupSnapshot snap = state.getGroups().get(group);
            long current = snap.getCommittedOffset();
            long maxOffset = snap.getEntries().size() - 1L;

            if (targetOffset < 0 || targetOffset > maxOffset) {
                rejectReplay(group, targetOffset, current, DecisionCode.REWIND_REJECTED_RANGE,
                        "offset rewind rejected: target=" + targetOffset
                                + " out of range [0," + maxOffset + "]");
            }
            if (targetOffset >= current + 1) {
                rejectReplay(group, targetOffset, current,
                        DecisionCode.REWIND_REJECTED_NOT_BEHIND,
                        "offset rewind rejected: target=" + targetOffset
                                + " is not earlier than committed+1=" + (current + 1)
                                + " (nothing earlier to replay)");
            }
            // 从 target 起的任何位置（含水线之后已租出但未确认的消息）只要还 IN_FLIGHT，
            // 回退后水位重新前进时就必然要“越过”这道未确认的坎，因此一律拒绝。
            for (GroupSnapshot.Entry in : snap.getEntries()) {
                if (in.offset < targetOffset) {
                    continue;
                }
                if (in.state == MessageState.IN_FLIGHT) {
                    rejectReplay(group, targetOffset, current,
                            DecisionCode.REWIND_REJECTED_INFLIGHT,
                            "offset rewind rejected: unacknowledged lease exists at offset="
                                    + in.offset + " state=IN_FLIGHT leaseId=" + in.currentLeaseId
                                    + " consumer=" + in.currentConsumerId
                                    + " (target=" + targetOffset + ", committed=" + current
                                    + "); committed offset left untouched");
                }
            }

            long t = now();
            List<ReplayRecord> reopened = new ArrayList<>();
            int resetPending = 0;
            int resetWaiting = 0;
            int reopenedSucceeded = 0;
            int reopenedDead = 0;
            for (GroupSnapshot.Entry e : snap.getEntries()) {
                if (e.offset < targetOffset) {
                    continue;
                }
                if (e.state == MessageState.IN_FLIGHT) {
                    // 校验阶段已保证不会走到这里
                    rejectReplay(group, targetOffset, current,
                            DecisionCode.REWIND_REJECTED_INFLIGHT,
                            "unexpected in-flight at " + e.offset);
                }
                if (e.state.isTerminal()) {
                    int replayNo = e.replayCount() + 1;
                    ReplayRecord rec = new ReplayRecord(replayNo, DeliveryKind.OFFSET_REWIND,
                            operator, reason, e.offset, e.messageId, t);
                    e.replays.add(rec);
                    e.state = MessageState.PENDING;
                    e.currentLeaseId = null;
                    e.currentConsumerId = null;
                    e.leaseExpiresAt = 0;
                    e.nextVisibleAt = t;
                    e.activeReplayNo = replayNo;
                    e.activeReplayKind = DeliveryKind.OFFSET_REWIND;
                    e.activeReplayOperator = operator;
                    reopened.add(rec);
                    if (e.succeededOnce) {
                        reopenedSucceeded++;
                    } else {
                        reopenedDead++;
                    }
                } else if (e.state == MessageState.RETRY_WAIT) {
                    // 非终态本就在水线之后；回退窗口扫到它时只取消退避等待，不记为重放动作
                    e.state = MessageState.PENDING;
                    e.nextVisibleAt = t;
                    resetWaiting++;
                } else if (e.state == MessageState.PENDING) {
                    resetPending++;
                }
            }
            snap.setCommittedOffset(targetOffset - 1);
            emit(group, null, targetOffset, DecisionCode.OFFSET_REWIND_ACCEPTED,
                    "committed offset rewound by operator: " + current + " -> "
                            + (targetOffset - 1) + " (replay starts at target=" + targetOffset
                            + "); terminal messages reopened=" + reopened.size()
                            + " (succeeded=" + reopenedSucceeded
                            + " keep their single-effect guarantee via idempotency guard; dead="
                            + reopenedDead
                            + " stay under the same maxAttempts=" + cfg.maxAttempts() + ")"
                            + " waitingBackoffCleared=" + resetWaiting
                            + " alreadyPending=" + resetPending
                            + " operator=" + operator + " reason=" + reason
                            + " (only this group is affected)");
            store.persist(state);
            return reopened;
        } finally {
            lock.unlock();
        }
    }

    private void rejectReplay(String group, long target, long current,
                              DecisionCode code, String detail) {
        emit(group, null, target, code, detail);
        throw new ReplayRejectedException(group, target, current, code, detail);
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

    /**
     * 业务幂等护栏：该消息是否已在本组成功生效过（含本次进程启动前的历史）。
     * 处理器在进度回退重放场景下应先查询本方法：返回 true 时跳过业务写操作、直接返回成功，
     * broker 仍会记录 {@code DUPLICATE_EFFECT_SUPPRESSED} 轨迹，保证业务效果全组只算一次。
     */
    public boolean hasBusinessEffectTaken(String group, long offset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot.Entry e = state.getGroups().get(group).entryAt(offset);
            return e != null && e.succeededOnce;
        } finally {
            lock.unlock();
        }
    }

    /** 某条消息在本组内的全部运维重放记录（死信重投/进度回退），按时间顺序。 */
    public List<ReplayRecord> getReplays(String group, long offset) {
        lock.lock();
        try {
            requireGroup(group);
            GroupSnapshot.Entry e = state.getGroups().get(group).entryAt(offset);
            return e == null ? List.of() : new ArrayList<>(e.replays);
        } finally {
            lock.unlock();
        }
    }

    /** 本组内所有消息的运维重放记录（按位点、时间顺序）。 */
    public List<ReplayRecord> getAllReplays(String group) {
        lock.lock();
        try {
            requireGroup(group);
            List<ReplayRecord> all = new ArrayList<>();
            for (GroupSnapshot.Entry e : state.getGroups().get(group).getEntries()) {
                all.addAll(e.replays);
            }
            return all;
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
