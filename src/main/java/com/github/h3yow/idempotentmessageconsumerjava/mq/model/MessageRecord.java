package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 一条消息在<b>一个消费者组</b>上的完整处理记录。
 *
 * <p>该对象承载全部判定依据：当前状态、每次尝试（只追加）、当前有效投递句柄、
 * 租约到期时间、重试可见时间，以及业务真正生效的次数（同组内只允许为 0 或 1）。
 * broker 持锁访问，故使用可变字段；Jackson 用私有无参构造反序列化。
 */
public final class MessageRecord {

    /** 生命周期事件（区别于消费尝试）：恢复、租约过期、忽略重复等，只追加。 */
    public record Event(Instant at, String detail) {
    }

    private String messageId;
    private String topic;
    private String payload;
    private long offset;
    private Instant enqueuedAt;

    private MessageStatus status = MessageStatus.PENDING;
    private final List<DeliveryAttempt> attempts = new ArrayList<>();
    private final List<Event> events = new ArrayList<>();

    /** 当前唯一有效（在途）投递句柄；为 null 表示当前没有消费者持有它。 */
    private String currentDeliveryId;
    private String currentConsumerId;
    private Instant leasedUntil;
    /** 本次在途投递的派发时间（仅内存使用，不持久化）。 */
    @JsonIgnore
    private Instant lastDispatchedAt;
    /** 重试退避结束、消息重新可见的时间点。 */
    private Instant nextVisibleAt;

    private Instant completedAt;
    private String terminalReason = "";

    /** 业务回调被真正执行并生效的次数，幂等协议要求同组内至多为 1。 */
    private int businessEffectCount;

    public MessageRecord() {
    }

    public MessageRecord(String messageId, String topic, String payload, long offset, Instant now) {
        this.messageId = messageId;
        this.topic = topic;
        this.payload = payload;
        this.offset = offset;
        this.enqueuedAt = now;
        this.nextVisibleAt = now;
    }

    /**
     * 是否可以在 now 时刻被投递：
     * 未终态、当前无有效在途租约、且已到退避可见时间。
     */
    public boolean isDeliverable(Instant now) {
        if (status == MessageStatus.SUCCEEDED || status == MessageStatus.DEAD) {
            return false;
        }
        if (currentDeliveryId != null && leasedUntil != null && now.isBefore(leasedUntil)) {
            return false;
        }
        return nextVisibleAt == null || !now.isBefore(nextVisibleAt);
    }

    /** 标记为一次在途投递（首次或重试）。attemptNo 为本次尝试序号（从 1 开始）。 */
    public void markInflight(String deliveryId, String consumerId, int attemptNo,
                             Instant leasedUntil, Instant now) {
        this.lastDispatchedAt = now;
        this.currentDeliveryId = deliveryId;
        this.currentConsumerId = consumerId;
        this.leasedUntil = leasedUntil;
        this.status = MessageStatus.INFLIGHT;
        this.events.add(new Event(now, "dispatched attempt#" + attemptNo
                + " via delivery=" + deliveryId + " to consumer=" + consumerId
                + ", leaseUntil=" + leasedUntil));
    }

    /** 租约过期但未确认：作废当前句柄，立即可重新投递。 */
    public void invalidateLease(Instant now, String why) {
        if (currentDeliveryId != null) {
            events.add(new Event(now, "lease invalidated for delivery=" + currentDeliveryId
                    + ": " + why));
        }
        this.currentDeliveryId = null;
        this.currentConsumerId = null;
        this.leasedUntil = null;
        this.nextVisibleAt = now;
        if (this.status == MessageStatus.INFLIGHT) {
            this.status = attempts.isEmpty() ? MessageStatus.PENDING : MessageStatus.RETRYING;
        }
    }

    /** 追加一次已结束的尝试。 */
    public void appendAttempt(DeliveryAttempt attempt) {
        this.attempts.add(attempt);
    }

    /** 消费成功：进入 SUCCEEDED 终态并记录一次业务生效。 */
    public void markSucceeded(DeliveryAttempt attempt, Instant now) {
        appendAttempt(attempt);
        if (this.status != MessageStatus.SUCCEEDED) {
            this.businessEffectCount++;
        }
        this.status = MessageStatus.SUCCEEDED;
        this.completedAt = now;
        this.terminalReason = attempt.reason();
        this.currentDeliveryId = null;
        this.currentConsumerId = null;
        this.leasedUntil = null;
        this.events.add(new Event(now, "succeeded after attempt#" + attempt.attemptNo()));
    }

    /** 可重试失败但仍有剩余额度：进入 RETRYING，旧句柄作废，退避到期后重新可见。 */
    public void markRetry(DeliveryAttempt attempt, Instant nextVisibleAt, Instant now) {
        appendAttempt(attempt);
        this.status = MessageStatus.RETRYING;
        this.nextVisibleAt = nextVisibleAt;
        this.currentDeliveryId = null;
        this.currentConsumerId = null;
        this.leasedUntil = null;
        this.events.add(new Event(now, "retry scheduled after attempt#" + attempt.attemptNo()
                + ", nextVisibleAt=" + nextVisibleAt + ", reason='" + attempt.reason() + "'"));
    }

    /** 重试耗尽或不可重试失败：进入 DEAD 终态（死信），原始记录保留。 */
    public void markDead(DeliveryAttempt attempt, Instant now, String deadReason) {
        appendAttempt(attempt);
        this.status = MessageStatus.DEAD;
        this.completedAt = now;
        this.terminalReason = deadReason;
        this.currentDeliveryId = null;
        this.currentConsumerId = null;
        this.leasedUntil = null;
        this.events.add(new Event(now, "dead-lettered: " + deadReason
                + " (last attempt#" + attempt.attemptNo() + ", reason='" + attempt.reason() + "')"));
    }

    public void addEvent(Instant now, String detail) {
        this.events.add(new Event(now, detail));
    }

    public String messageId() { return messageId; }
    public String topic() { return topic; }
    public String payload() { return payload; }
    public long offset() { return offset; }
    public Instant enqueuedAt() { return enqueuedAt; }
    public MessageStatus status() { return status; }
    public List<DeliveryAttempt> attempts() { return attempts; }
    public List<Event> events() { return events; }
    public String currentDeliveryId() { return currentDeliveryId; }
    public String currentConsumerId() { return currentConsumerId; }
    public Instant leasedUntil() { return leasedUntil; }
    public Instant lastDispatchedAt() { return lastDispatchedAt; }
    public Instant nextVisibleAt() { return nextVisibleAt; }
    public Instant completedAt() { return completedAt; }
    public String terminalReason() { return terminalReason; }
    public int businessEffectCount() { return businessEffectCount; }

    public int attemptCount() { return attempts.size(); }

    public boolean isTerminal() {
        return status == MessageStatus.SUCCEEDED || status == MessageStatus.DEAD;
    }

    @Override
    public String toString() {
        return "MessageRecord{msg=" + messageId + ", topic=" + topic + ", offset=" + offset
                + ", status=" + status + ", attempts=" + attempts.size()
                + ", currentDelivery=" + currentDeliveryId
                + ", effectCount=" + businessEffectCount + "}";
    }
}
