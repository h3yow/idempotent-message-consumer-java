package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 业务处理器对一条消息的处理结论。
 *
 * <p>处理器除返回该结论外，也可以直接抛出异常：抛出任意异常一律按
 * {@link Verdict#RETRY 可重试失败}处理；需要立即进入死信时必须显式返回
 * {@link #dead(String)}，使失败原因可区分、可追溯。</p>
 */
public final class ConsumeOutcome {

    public enum Verdict {
        /** 业务已生效。 */
        SUCCESS,
        /** 本次失败可重试，交由退避策略安排下一次投递。 */
        RETRY,
        /** 不可重试（或业务判定无效消息），直接进入死信。 */
        DEAD
    }

    private final Verdict verdict;
    private final String reason;

    private ConsumeOutcome(Verdict verdict, String reason) {
        this.verdict = verdict;
        this.reason = reason;
    }

    public static ConsumeOutcome success() {
        return new ConsumeOutcome(Verdict.SUCCESS, "HANDLER_RETURNED_SUCCESS");
    }

    public static ConsumeOutcome retry(String reason) {
        return new ConsumeOutcome(Verdict.RETRY, reason);
    }

    public static ConsumeOutcome dead(String reason) {
        return new ConsumeOutcome(Verdict.DEAD, reason);
    }

    public Verdict verdict() {
        return verdict;
    }

    public String reason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ConsumeOutcome{" + verdict + ", reason='" + reason + "'}";
    }
}
