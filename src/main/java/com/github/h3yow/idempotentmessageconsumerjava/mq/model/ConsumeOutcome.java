package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 业务消费回调对一条消息的判定结果。
 * <ul>
 *   <li>{@link AttemptResult#CONSUMED}：业务生效成功，消息在该组内永久完成；</li>
 *   <li>{@link AttemptResult#RETRYABLE_FAILURE}：可重试失败，按退避策略再次投递；</li>
 *   <li>{@link AttemptResult#NON_RETRYABLE_FAILURE}：不可重试失败，直接进入死信。</li>
 * </ul>
 */
public final class ConsumeOutcome {

    private final AttemptResult result;
    private final String reason;

    private ConsumeOutcome(AttemptResult result, String reason) {
        this.result = result;
        this.reason = reason == null ? "" : reason;
    }

    public static ConsumeOutcome success() {
        return new ConsumeOutcome(AttemptResult.CONSUMED, "business consumed");
    }

    public static ConsumeOutcome retryable(String reason) {
        return new ConsumeOutcome(AttemptResult.RETRYABLE_FAILURE, reason);
    }

    public static ConsumeOutcome nonRetryable(String reason) {
        return new ConsumeOutcome(AttemptResult.NON_RETRYABLE_FAILURE, reason);
    }

    public AttemptResult result() {
        return result;
    }

    public String reason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ConsumeOutcome{result=" + result + ", reason='" + reason + "'}";
    }
}
