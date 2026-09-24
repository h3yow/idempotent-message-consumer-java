package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.github.h3yow.idempotentmessageconsumerjava.mq.core.MessageConsumer;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Delivery;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryAttempt;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.IgnoreReason;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageEnvelope;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.OffsetCommitResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.RejectReason;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.BrokerState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.StateStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.TopicConsumeState;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 本地幂等消息消费组件。
 *
 * <p>用内存/本地文件模拟消息中间件，在单进程内通过一把锁串行化所有状态变更，
 * 提供可解释的最终一致语义：
 * <ul>
 *   <li><b>幂等</b>：同一消息在同一消费者组内业务至多生效一次（{@code businessEffectCount}）；
 *       重复投递/重复消费返回 {@link DeliveryAckResult} 并带可区分的 {@link IgnoreReason}。</li>
 *   <li><b>重试与死信</b>：可重试失败按 {@link RetryPolicy} 退避后重新投递；
 *       超过最大尝试次数或不可重试失败进入死信；每次尝试只追加在 {@code attempts} 中。</li>
 *   <li><b>位点</b>：只能提交连续已确认前缀；回退与越过未确认消息被拒绝。</li>
 *   <li><b>重启恢复</b>：每次状态变更都持久化；重启后在途消息租约过期即重新投递，
 *       旧投递句柄失效，不会重复生效。</li>
 * </ul>
 */
public final class IdempotentBroker {

    /** 入队结果：区分首次入队与重复入队。 */
    public record EnqueueResult(boolean accepted, boolean duplicate, String messageId, long offset) {
    }

    /** ack/nack 判定结果。record 为当前最新处理记录（可能为 null）。 */
    public record DeliveryAckResult(DeliveryOutcome outcome, IgnoreReason ignoreReason,
                                    int attempt, MessageRecord record) {

        public boolean businessApplied() {
            return outcome == DeliveryOutcome.ACK_ACCEPTED;
        }
    }

    private final StateStore store;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final BrokerState state;

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, MessageConsumer> subscriptions = new ConcurrentHashMap<>();

    /** deliveryId -> (group, messageId)：识别在途/过期/未知句柄。 */
    private final Map<String, String> deliveryGroup = new ConcurrentHashMap<>();
    private final Map<String, String> deliveryMessage = new ConcurrentHashMap<>();
    /** 已结算投递的历史索引 deliveryId -> group|messageId（仅内存），用于区分重复确认原因。 */
    private final Map<String, String> settledDeliveryGroup = new ConcurrentHashMap<>();
    private final Map<String, String> settledDeliveryMessage = new ConcurrentHashMap<>();

    public IdempotentBroker(StateStore store, RetryPolicy retryPolicy) {
        this(store, retryPolicy, Clock.systemUTC());
    }

    public IdempotentBroker(StateStore store, RetryPolicy retryPolicy, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.state = store.load();
        rebuildDeliveryIndex();
    }

    /** 重启后从记录中重建在途句柄索引（只收录当前仍有效的在途句柄）。 */
    private void rebuildDeliveryIndex() {
        deliveryGroup.clear();
        deliveryMessage.clear();
        // 重启后所有旧句柄失效：历史索引也不保留，旧句柄确认一律 UNKNOWN_DELIVERY
        settledDeliveryGroup.clear();
        settledDeliveryMessage.clear();
        Instant now = Instant.now(clock);
        boolean any = false;
        for (Map.Entry<String, com.github.h3yow.idempotentmessageconsumerjava.mq.store.GroupState> ge
                : state.groups().entrySet()) {
            for (Map.Entry<String, TopicConsumeState> te : ge.getValue().topics().entrySet()) {
                for (MessageRecord r : te.getValue().records().values()) {
                    if (r.currentDeliveryId() != null) {
                        // 重启即视为原消费者不再持有有效处理权：过期租约，待 poll 时重投
                        r.invalidateLease(now, "broker restarted: inflight lease recovered");
                        any = true;
                    }
                }
            }
        }
        if (any) {
            store.save(state);
        }
    }

    public void subscribe(String group, MessageConsumer consumer) {
        subscriptions.put(Objects.requireNonNull(group), Objects.requireNonNull(consumer));
    }

    private Instant now() {
        return Instant.now(clock);
    }

    private void persist() {
        store.save(state);
    }

    /** 入队（自动生成 messageId）。 */
    public EnqueueResult enqueue(String topic, String payload) {
        return enqueue(topic, null, payload);
    }

    /**
     * 入队。messageId 非空时启用入队去重：同一 messageId 重复入队会被识别为 duplicate，
     * 返回原始位点且不再写入日志。
     */
    public EnqueueResult enqueue(String topic, String messageId, String payload) {
        lock.lock();
        try {
            Instant ts = now();
            if (messageId != null) {
                String existingTopic = state.published().get(messageId);
                if (existingTopic != null) {
                    String dupId = messageId;
                    long existingOffset = -1L;
                    for (MessageEnvelope e : state.log(existingTopic)) {
                        if (e.messageId().equals(dupId)) {
                            existingOffset = e.offset();
                            break;
                        }
                    }
                    return new EnqueueResult(false, true, dupId, existingOffset);
                }
            } else {
                messageId = "msg-" + Long.toHexString(state.nextSeq())
                        + "-" + UUID.randomUUID().toString().substring(0, 8);
            }
            List<MessageEnvelope> log = state.log(topic);
            long offset = log.size();
            log.add(new MessageEnvelope(messageId, topic, payload == null ? "" : payload, offset, ts));
            state.published().put(messageId, topic);
            persist();
            return new EnqueueResult(true, false, messageId, offset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 拉取并按消费者组分发。会先处理租约过期（含重启恢复）的在途消息，再投递
     * 已到退避时间的可投递消息。每次投递生成新的 deliveryId 且 attemptNo 递增。
     */
    public List<Delivery> poll(String group, String topic, String consumerId, int max) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(topic);
        Objects.requireNonNull(consumerId);
        if (max <= 0) {
            return List.of();
        }
        lock.lock();
        try {
            Instant now = now();
            TopicConsumeState tcs = state.group(group).topic(topic);
            List<Delivery> out = new ArrayList<>();

            for (MessageEnvelope env : state.log(topic)) {
                if (out.size() >= max) {
                    break;
                }
                MessageRecord r = tcs.record(env.messageId());
                if (r == null) {
                    r = new MessageRecord(env.messageId(), topic, env.payload(), env.offset(), now);
                    tcs.putRecord(r);
                }
                // 在途租约过期（宕机/重启）：作废旧句柄
                if (r.currentDeliveryId() != null && r.leasedUntil() != null
                        && !now.isBefore(r.leasedUntil())) {
                    String old = r.currentDeliveryId();
                    r.invalidateLease(now, "lease timed out before ack");
                    deliveryGroup.remove(old);
                    deliveryMessage.remove(old);
                    r.addEvent(now, "message will be redelivered to consumer=" + consumerId);
                }
                if (!r.isDeliverable(now)) {
                    continue;
                }
                int attemptNo = r.attemptCount() + 1;
                String deliveryId = "dlv-" + UUID.randomUUID();
                Instant leaseUntil = now.plus(retryPolicy.visibilityLease());
                r.markInflight(deliveryId, consumerId, attemptNo, leaseUntil, now);
                deliveryGroup.put(deliveryId, group);
                deliveryMessage.put(deliveryId, r.messageId());
                out.add(new Delivery(deliveryId, group, topic, r.messageId(), r.payload(),
                        r.offset(), attemptNo, consumerId, now));
            }
            persist();
            return out;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 确认一次投递。outcome 为业务判定（成功 / 可重试失败 / 不可重试失败）。
     * 返回的 {@link DeliveryAckResult} 完整描述判定结果与依据。
     */
    public DeliveryAckResult ack(String group, String deliveryId, ConsumeOutcome outcome) {
        Objects.requireNonNull(group);
        Objects.requireNonNull(deliveryId);
        Objects.requireNonNull(outcome);
        lock.lock();
        try {
            Instant now = now();

            // 1) 当前在途句柄？
            String ownerGroup = deliveryGroup.get(deliveryId);
            String messageId = deliveryMessage.get(deliveryId);
            if (ownerGroup == null || messageId == null || !ownerGroup.equals(group)) {
                return classifySettledOrUnknown(group, deliveryId);
            }

            MessageRecord r = findRecord(group, messageId);
            if (r == null || !deliveryId.equals(r.currentDeliveryId())) {
                // 理论上在途索引与记录不一致（如重启后），交给历史/未知判定
                return classifySettledOrUnknown(group, deliveryId);
            }

            // 2) 当前有效句柄：执行判定
            int attemptNo = r.attemptCount() + 1;
            Instant dispatchedAt = r.lastDispatchedAt() == null ? now : r.lastDispatchedAt();
            DeliveryAttempt attempt = new DeliveryAttempt(deliveryId, r.currentConsumerId(),
                    attemptNo, dispatchedAt, now, outcome.result(), outcome.reason());

            // 句柄即将结算：从在途索引摘除并转入历史索引，保证重放时能给出可区分原因
            deliveryGroup.remove(deliveryId);
            deliveryMessage.remove(deliveryId);
            settledDeliveryGroup.put(deliveryId, group);
            settledDeliveryMessage.put(deliveryId, messageId);

            if (outcome.result() == AttemptResult.CONSUMED) {
                r.markSucceeded(attempt, now);
                persist();
                return new DeliveryAckResult(DeliveryOutcome.ACK_ACCEPTED, null, attemptNo, r);
            }

            if (outcome.result() == AttemptResult.NON_RETRYABLE_FAILURE) {
                r.markDead(attempt, now, "non-retryable failure: " + outcome.reason());
                persist();
                return new DeliveryAckResult(DeliveryOutcome.DEAD_LETTERED, null, attemptNo, r);
            }

            // RETRYABLE_FAILURE：本次是第 attemptNo 次尝试，若已达上限则进死信
            if (attemptNo >= retryPolicy.maxAttempts()) {
                r.markDead(attempt, now, "retry budget exhausted after " + attemptNo
                        + " attempt(s), maxAttempts=" + retryPolicy.maxAttempts());
                persist();
                return new DeliveryAckResult(DeliveryOutcome.DEAD_LETTERED, null, attemptNo, r);
            }
            Duration wait = retryPolicy.backoffAfter(attemptNo);
            r.markRetry(attempt, now.plus(wait), now);
            persist();
            return new DeliveryAckResult(DeliveryOutcome.RETRY_SCHEDULED, null, attemptNo, r);
        } finally {
            lock.unlock();
        }
    }

    /** 对不在在途索引中的句柄做历史判定：已成功 / 已死信 / 未知（含重试中、重启后失效）。 */
    private DeliveryAckResult classifySettledOrUnknown(String group, String deliveryId) {
        String settledGroup = settledDeliveryGroup.get(deliveryId);
        String settledMessageId = settledDeliveryMessage.get(deliveryId);
        if (settledGroup != null && settledGroup.equals(group) && settledMessageId != null) {
            MessageRecord hist = findRecord(group, settledMessageId);
            if (hist != null) {
                if (hist.status() == com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageStatus.SUCCEEDED) {
                    return new DeliveryAckResult(DeliveryOutcome.DUPLICATE_IGNORED,
                            IgnoreReason.ALREADY_SUCCEEDED, hist.attemptCount(), hist);
                }
                if (hist.status() == com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageStatus.DEAD) {
                    return new DeliveryAckResult(DeliveryOutcome.DUPLICATE_IGNORED,
                            IgnoreReason.MESSAGE_IN_DLQ, hist.attemptCount(), hist);
                }
            }
        }
        return new DeliveryAckResult(DeliveryOutcome.DUPLICATE_IGNORED,
                IgnoreReason.UNKNOWN_DELIVERY, 0, null);
    }

    private MessageRecord findRecord(String group, String messageId) {
        var gs = state.groups().get(group);
        if (gs == null) {
            return null;
        }
        for (var tcs : gs.topics().values()) {
            MessageRecord r = tcs.record(messageId);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    /**
     * 提交位点。规则：只能把位点推进到“连续已确认前缀”的下一个位置。
     * <ul>
     *   <li>requested &lt; committed：拒绝 {@link RejectReason#REGRESSED}；</li>
     *   <li>requested == committed：幂等 no-op，接受；</li>
     *   <li>存在 committed &lt;= offset &lt; requested 的消息未终态：
     *       拒绝 {@link RejectReason#GAP_WITH_UNACKED}；</li>
     *   <li>requested 超出日志长度：拒绝 {@link RejectReason#UNKNOWN_MESSAGE}。</li>
     * </ul>
     */
    public OffsetCommitResult commitOffset(String group, String topic, long requested) {
        lock.lock();
        try {
            TopicConsumeState tcs = state.group(group).topic(topic);
            long current = tcs.committedOffset();
            List<MessageEnvelope> log = state.log(topic);

            if (requested == current) {
                return OffsetCommitResult.noop(current);
            }
            if (requested < current) {
                var res = OffsetCommitResult.rejected(requested, current, RejectReason.REGRESSED,
                        "cannot move committed offset backwards; committed=" + current);
                persist();
                return res;
            }
            if (requested > log.size()) {
                var res = OffsetCommitResult.rejected(requested, current, RejectReason.UNKNOWN_MESSAGE,
                        "requested offset " + requested + " beyond log size " + log.size());
                persist();
                return res;
            }
            // 找出 [current, requested) 内所有未终态消息
            List<String> gaps = new ArrayList<>();
            for (long o = current; o < requested; o++) {
                MessageEnvelope env = log.get((int) o);
                MessageRecord r = tcs.record(env.messageId());
                if (r == null || !r.isTerminal()) {
                    gaps.add(env.messageId() + "@" + o + "("
                            + (r == null ? "NEVER_DELIVERED" : r.status()) + ")");
                }
            }
            if (!gaps.isEmpty()) {
                var res = OffsetCommitResult.rejected(requested, current,
                        RejectReason.GAP_WITH_UNACKED,
                        "unacked messages block the offset: " + gaps);
                persist();
                return res;
            }
            tcs.committedOffset(requested);
            persist();
            return OffsetCommitResult.accepted(requested, current, requested);
        } finally {
            lock.unlock();
        }
    }

    public long committedOffset(String group, String topic) {
        lock.lock();
        try {
            return state.group(group).topic(topic).committedOffset();
        } finally {
            lock.unlock();
        }
    }

    /** 某消费者组内所有死信记录。 */
    public List<MessageRecord> deadLetters(String group) {
        lock.lock();
        try {
            List<MessageRecord> out = new ArrayList<>();
            com.github.h3yow.idempotentmessageconsumerjava.mq.store.GroupState gs =
                    state.groups().get(group);
            if (gs == null) {
                return out;
            }
            for (TopicConsumeState tcs : gs.topics().values()) {
                for (MessageRecord r : tcs.records().values()) {
                    if (r.status() == com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageStatus.DEAD) {
                        out.add(r);
                    }
                }
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** 取某组某主题的全部处理记录（按插入顺序），供测试与排障解释。 */
    public List<MessageRecord> records(String group, String topic) {
        lock.lock();
        try {
            TopicConsumeState tcs = state.group(group).topic(topic);
            return new ArrayList<>(tcs.records().values());
        } finally {
            lock.unlock();
        }
    }

    /** 便捷方法：poll 后对每条投递执行已订阅的业务回调并 ack，返回每条的判定。 */
    public List<DeliveryAckResult> consumeOnce(String group, String topic,
                                               String consumerId, int max) {
        MessageConsumer consumer = subscriptions.get(group);
        if (consumer == null) {
            throw new IllegalStateException("no consumer subscribed for group " + group);
        }
        List<Delivery> deliveries = poll(group, topic, consumerId, max);
        List<DeliveryAckResult> results = new ArrayList<>();
        for (Delivery d : deliveries) {
            ConsumeOutcome outcome;
            try {
                outcome = consumer.consume(d.messageId(), d.payload());
                if (outcome == null) {
                    outcome = ConsumeOutcome.success();
                }
            } catch (RuntimeException e) {
                outcome = ConsumeOutcome.retryable("consumer threw "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            results.add(ack(group, d.deliveryId(), outcome));
        }
        return results;
    }

    public void close() {
        lock.lock();
        try {
            persist();
        } finally {
            lock.unlock();
        }
    }
}
