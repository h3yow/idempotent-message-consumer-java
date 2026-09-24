package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * 主题日志中的一条原始消息（与消费者组无关，重启后原样保留）。
 * 位点 {@code offset} 在单个 topic 内单调递增、从 0 开始。
 */
public final class MessageEnvelope {

    private String messageId;
    private String topic;
    private String payload;
    private long offset;
    private Instant enqueuedAt;

    public MessageEnvelope() {
    }

    @JsonCreator
    public MessageEnvelope(@JsonProperty("messageId") String messageId,
                           @JsonProperty("topic") String topic,
                           @JsonProperty("payload") String payload,
                           @JsonProperty("offset") long offset,
                           @JsonProperty("enqueuedAt") Instant enqueuedAt) {
        this.messageId = messageId;
        this.topic = topic;
        this.payload = payload;
        this.offset = offset;
        this.enqueuedAt = enqueuedAt;
    }

    public String messageId() { return messageId; }
    public String topic() { return topic; }
    public String payload() { return payload; }
    public long offset() { return offset; }
    public Instant enqueuedAt() { return enqueuedAt; }

    @Override
    public String toString() {
        return "Envelope{id=" + messageId + ", topic=" + topic + ", offset=" + offset + "}";
    }
}
