package com.github.h3yow.idempotentmessageconsumerjava.mq.retry;

import com.github.h3yow.idempotentmessageconsumerjava.mq.core.BackoffStrategy;

import java.time.Duration;

/**
 * 指数退避：delay(n) = min(initial * multiplier^(n-1), max)。
 * 传入的 failedAttempts 为已失败次数（1 表示刚失败第一次）。
 */
public final class ExponentialBackoff implements BackoffStrategy {

    private final Duration initial;
    private final double multiplier;
    private final Duration max;

    public ExponentialBackoff(Duration initial, double multiplier, Duration max) {
        this.initial = initial;
        this.multiplier = multiplier;
        this.max = max;
    }

    @Override
    public Duration delayFor(int failedAttempts) {
        long millis = initial.toMillis();
        for (int i = 1; i < failedAttempts; i++) {
            millis = (long) (millis * multiplier);
            if (millis >= max.toMillis()) {
                return max;
            }
        }
        Duration computed = Duration.ofMillis(millis);
        return computed.compareTo(max) > 0 ? max : computed;
    }
}
