package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

/**
 * 死信重投被拒绝时抛出。携带可区分的拒绝编码：
 * {@link DecisionCode#REDELIVER_REJECTED_UNKNOWN_MESSAGE}（组内无此消息）或
 * {@link DecisionCode#REDELIVER_REJECTED_NOT_DEAD}（消息不是死信终态）。
 * 被拒绝时消息状态、位点与死信记录均保持原样。
 */
public class RedeliverRejectedException extends RuntimeException {

    private final transient String group;
    private final transient String messageId;
    private final transient DecisionCode code;

    public RedeliverRejectedException(String group, String messageId,
                                      DecisionCode code, String detail) {
        super("dead-letter redelivery rejected for group='" + group
                + "', messageId=" + messageId + ", code=" + code + ": " + detail);
        this.group = group;
        this.messageId = messageId;
        this.code = code;
    }

    public String group() {
        return group;
    }

    public String messageId() {
        return messageId;
    }

    public DecisionCode code() {
        return code;
    }
}
