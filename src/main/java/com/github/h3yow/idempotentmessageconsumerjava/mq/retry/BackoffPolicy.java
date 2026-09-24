package com.github.h3yow.idempotentmessageconsumerjava.mq.retry;

/**
 * 退避策略：给定“刚刚失败的是第几次尝试”，计算下一次允许投递前的等待毫秒数。
 */
@FunctionalInterface
public interface BackoffPolicy {

    /**
     * @param failedAttemptNo 刚结束的失败尝试序号（首次失败为 1）
     * @return 下一次投递前需要等待的毫秒数
     */
    long nextBackoffMillis(int failedAttemptNo);
}
