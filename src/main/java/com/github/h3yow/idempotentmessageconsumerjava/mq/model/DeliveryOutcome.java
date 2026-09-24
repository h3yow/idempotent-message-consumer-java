package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/** ack/nack 的判定结果类型。 */
public enum DeliveryOutcome {
    ACK_ACCEPTED,
    DUPLICATE_IGNORED,
    RETRY_SCHEDULED,
    DEAD_LETTERED
}
