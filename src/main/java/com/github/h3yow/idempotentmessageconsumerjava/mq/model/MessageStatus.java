package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/** 消息在某个消费者组上的处理状态。 */
public enum MessageStatus {
    PENDING,
    INFLIGHT,
    SUCCEEDED,
    RETRYING,
    DEAD
}
