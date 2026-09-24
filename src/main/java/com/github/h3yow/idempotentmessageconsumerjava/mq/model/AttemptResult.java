package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/** 单次消费尝试的结果。 */
public enum AttemptResult {
    CONSUMED,
    RETRYABLE_FAILURE,
    NON_RETRYABLE_FAILURE
}
