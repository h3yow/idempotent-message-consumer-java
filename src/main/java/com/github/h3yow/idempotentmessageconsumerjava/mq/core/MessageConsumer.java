package com.github.h3yow.idempotentmessageconsumerjava.mq.core;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;

/** 业务消费回调。 */
@FunctionalInterface
public interface MessageConsumer {
    ConsumeOutcome consume(String messageId, String payload);
}
