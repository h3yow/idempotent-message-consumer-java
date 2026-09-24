package com.github.h3yow.idempotentmessageconsumerjava.mq.core;

import java.time.Duration;

/** 退避策略。 */
@FunctionalInterface
public interface BackoffStrategy {
    Duration delayFor(int attempt);
}
