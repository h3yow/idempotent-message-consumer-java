package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/** 重复投递/重复消费被幂等忽略时的可区分原因。 */
public enum IgnoreReason {
    ALREADY_SUCCEEDED,
    IN_FLIGHT_ELSEWHERE,
    MESSAGE_IN_DLQ,
    UNKNOWN_DELIVERY
}
