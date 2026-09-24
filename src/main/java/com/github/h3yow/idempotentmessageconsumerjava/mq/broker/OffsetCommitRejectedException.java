package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

/**
 * 位点提交被拒绝时抛出。携带拒绝编码与判定依据，区分回退提交与跨越未确认消息两种情形。
 */
public class OffsetCommitRejectedException extends RuntimeException {

    private final transient String group;
    private final transient long requestedOffset;
    private final transient long currentCommittedOffset;
    private final transient DecisionCode code;

    public OffsetCommitRejectedException(String group,
                                         long requestedOffset,
                                         long currentCommittedOffset,
                                         DecisionCode code,
                                         String detail) {
        super("offset commit rejected for group='" + group
                + "', requested=" + requestedOffset
                + ", committed=" + currentCommittedOffset
                + ", code=" + code + ": " + detail);
        this.group = group;
        this.requestedOffset = requestedOffset;
        this.currentCommittedOffset = currentCommittedOffset;
        this.code = code;
    }

    public String group() {
        return group;
    }

    public long requestedOffset() {
        return requestedOffset;
    }

    public long currentCommittedOffset() {
        return currentCommittedOffset;
    }

    public DecisionCode code() {
        return code;
    }
}
