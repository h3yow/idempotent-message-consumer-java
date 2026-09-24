package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import java.time.Instant;

/**
 * 一次投递的句柄。消费者必须持有有效句柄才能确认消息。
 *
 * <p>每次重新投递（重试或重启恢复）都会生成新的 {@code deliveryId} 且 {@code attemptNo} 递增；
 * 旧句柄在新句柄发出后即失效，用旧句柄确认会得到 {@link IgnoreReason#UNKNOWN_DELIVERY}。
 */
public final class Delivery {

    private final String deliveryId;
    private final String group;
    private final String topic;
    private final String messageId;
    private final String payload;
    private final long offset;
    private final int attemptNo;
    private final String consumerId;
    private final Instant dispatchedAt;

    public Delivery(String deliveryId, String group, String topic, String messageId,
                    String payload, long offset, int attemptNo,
                    String consumerId, Instant dispatchedAt) {
        this.deliveryId = deliveryId;
        this.group = group;
        this.topic = topic;
        this.messageId = messageId;
        this.payload = payload;
        this.offset = offset;
        this.attemptNo = attemptNo;
        this.consumerId = consumerId;
        this.dispatchedAt = dispatchedAt;
    }

    public String deliveryId() { return deliveryId; }
    public String group() { return group; }
    public String topic() { return topic; }
    public String messageId() { return messageId; }
    public String payload() { return payload; }
    public long offset() { return offset; }
    public int attemptNo() { return attemptNo; }
    public String consumerId() { return consumerId; }
    public Instant dispatchedAt() { return dispatchedAt; }

    @Override
    public String toString() {
        return "Delivery{id=" + deliveryId + ", msg=" + messageId
                + ", topic=" + topic + ", offset=" + offset
                + ", attempt=" + attemptNo + ", consumer=" + consumerId + "}";
    }
}
