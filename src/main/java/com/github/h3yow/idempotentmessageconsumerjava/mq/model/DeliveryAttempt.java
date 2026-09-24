package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * 一次消费尝试的完整追溯记录：谁在什么时候拿到消息、返回了什么结果、原因是什么。
 * 记录只追加不修改，保证每次尝试可解释。
 */
public final class DeliveryAttempt {

    private final String deliveryId;
    private final String consumerId;
    private final int attemptNo;
    private final Instant dispatchedAt;
    private final Instant finishedAt;
    private final AttemptResult result;
    private final String reason;

    @JsonCreator
    public DeliveryAttempt(@JsonProperty("deliveryId") String deliveryId,
                           @JsonProperty("consumerId") String consumerId,
                           @JsonProperty("attemptNo") int attemptNo,
                           @JsonProperty("dispatchedAt") Instant dispatchedAt,
                           @JsonProperty("finishedAt") Instant finishedAt,
                           @JsonProperty("result") AttemptResult result,
                           @JsonProperty("reason") String reason) {
        this.deliveryId = deliveryId;
        this.consumerId = consumerId;
        this.attemptNo = attemptNo;
        this.dispatchedAt = dispatchedAt;
        this.finishedAt = finishedAt;
        this.result = result;
        this.reason = reason == null ? "" : reason;
    }

    public String deliveryId() { return deliveryId; }
    public String consumerId() { return consumerId; }
    public int attemptNo() { return attemptNo; }
    public Instant dispatchedAt() { return dispatchedAt; }
    public Instant finishedAt() { return finishedAt; }
    public AttemptResult result() { return result; }
    public String reason() { return reason; }

    @Override
    public String toString() {
        return "Attempt#" + attemptNo + "{delivery=" + deliveryId
                + ", consumer=" + consumerId
                + ", at=" + dispatchedAt
                + ", result=" + result
                + ", reason='" + reason + "'}";
    }
}
