package com.github.h3yow.idempotentmessageconsumerjava.mq.retry;

/**
 * 指数退避：{@code delay = min(base * multiplier^(n-1), max)}，可加抖动。
 *
 * @param baseMillis      首次失败后的基础等待
 * @param multiplier      乘数（&gt;=1）
 * @param maxMillis       等待上限
 * @param jitterMillis    抖动上限（在计算结果上叠加 [0, jitter) 的随机量，再截断到 max）
 */
public record ExponentialBackoff(long baseMillis,
                                 double multiplier,
                                 long maxMillis,
                                 long jitterMillis) implements BackoffPolicy {

    public ExponentialBackoff {
        if (baseMillis < 0 || maxMillis < baseMillis || multiplier < 1.0 || jitterMillis < 0) {
            throw new IllegalArgumentException("invalid backoff parameters");
        }
    }

    /** 无抖动的便捷构造。 */
    public ExponentialBackoff(long baseMillis, double multiplier, long maxMillis) {
        this(baseMillis, multiplier, maxMillis, 0);
    }

    @Override
    public long nextBackoffMillis(int failedAttemptNo) {
        double raw = baseMillis * Math.pow(multiplier, Math.max(0, failedAttemptNo - 1));
        long delay = (long) Math.min(raw, (double) maxMillis);
        if (jitterMillis > 0) {
            delay += (long) (Math.random() * jitterMillis);
            delay = Math.min(delay, maxMillis);
        }
        return delay;
    }
}
