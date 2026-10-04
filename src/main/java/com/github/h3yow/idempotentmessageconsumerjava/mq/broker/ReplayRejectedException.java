package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

/**
 * 消费进度重放（回退）被拒绝时抛出。携带可区分的拒绝编码：
 * {@link DecisionCode#REPLAY_REJECTED_RANGE}（目标位点越界）或
 * {@link DecisionCode#REPLAY_REJECTED_NOT_BACKWARD}（目标不是回退，即 &gt;= 当前已提交位点）。
 * 被拒绝时位点与所有消息状态保持原样，不留半截进度。
 */
public class ReplayRejectedException extends RuntimeException {

    private final transient String group;
    private final transient long requestedOffset;
    private final transient long currentCommittedOffset;
    private final transient DecisionCode code;

    public ReplayRejectedException(String group,
                                   long requestedOffset,
                                   long currentCommittedOffset,
                                   DecisionCode code,
                                   String detail) {
        super("replay rejected for group='" + group
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
