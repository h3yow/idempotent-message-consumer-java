package com.github.h3yow.idempotentmessageconsumerjava.mq.retry;

import com.github.h3yow.idempotentmessageconsumerjava.mq.core.BackoffStrategy;

import java.time.Duration;

/**
 * 重试策略。
 *
 * <ul>
 *   <li>{@code maxAttempts}：一条消息允许的<b>总投递次数</b>（含首次）。
 *       例如 maxAttempts=3 表示首次 + 最多 2 次重试；第 3 次仍失败则进入死信。</li>
 *   <li>{@code backoff}：第 n 次失败后的退避时长。</li>
 *   <li>{@code visibilityLease}：投递租约时长。超过租约仍未确认的消息视为消费者宕机，可重新投递。</li>
 * </ul>
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final BackoffStrategy backoff;
    private final Duration visibilityLease;

    public RetryPolicy(int maxAttempts, BackoffStrategy backoff, Duration visibilityLease) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.backoff = backoff;
        this.visibilityLease = visibilityLease;
    }

    /** 便捷默认策略：指数退避、固定租约。 */
    public static RetryPolicy of(int maxAttempts, Duration initialBackoff,
                                 double multiplier, Duration maxBackoff, Duration lease) {
        return new RetryPolicy(maxAttempts,
                new ExponentialBackoff(initialBackoff, multiplier, maxBackoff), lease);
    }

    public int maxAttempts() { return maxAttempts; }

    public BackoffStrategy backoff() { return backoff; }

    public Duration visibilityLease() { return visibilityLease; }

    /** 已完成 failedAttempts 次失败后，下一次重试需要等待的时长。 */
    public Duration backoffAfter(int failedAttempts) {
        Duration d = backoff.delayFor(failedAttempts);
        return d == null ? Duration.ZERO : d;
    }

}
