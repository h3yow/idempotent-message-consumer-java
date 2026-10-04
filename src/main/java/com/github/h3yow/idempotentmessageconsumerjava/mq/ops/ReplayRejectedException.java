package com.github.h3yow.idempotentmessageconsumerjava.mq.ops;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;

/**
 * 运维重放操作（死信重投 / 消费进度回退）被拒绝时抛出。
 * 不同拒绝原因以 {@link DecisionCode} 区分，拒绝时状态保持原样、不落盘任何半完成进度。
 */
public class ReplayRejectedException extends RuntimeException {

    private final transient String group;
    private final transient long targetOffset;
    private final transient long currentCommittedOffset;
    private final transient DecisionCode code;

    public ReplayRejectedException(String group,
                                   long targetOffset,
                                   long currentCommittedOffset,
                                   DecisionCode code,
                                   String detail) {
        super("replay operation rejected for group='" + group
                + "', target=" + targetOffset
                + ", committed=" + currentCommittedOffset
                + ", code=" + code + ": " + detail);
        this.group = group;
        this.targetOffset = targetOffset;
        this.currentCommittedOffset = currentCommittedOffset;
        this.code = code;
    }

    public String group() {
        return group;
    }

    public long targetOffset() {
        return targetOffset;
    }

    public long currentCommittedOffset() {
        return currentCommittedOffset;
    }

    public DecisionCode code() {
        return code;
    }
}
