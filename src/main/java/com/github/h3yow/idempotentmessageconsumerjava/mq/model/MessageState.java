package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 消息在单个消费者组内的处理状态。
 *
 * <ul>
 *   <li>{@link #PENDING}：已入组、尚未被租出处理；</li>
 *   <li>{@link #IN_FLIGHT}：已被某个消费者租出，等待 ack/nack；</li>
 *   <li>{@link #RETRY_WAIT}：可重试失败，处于退避等待中，到期可再次投递；</li>
 *   <li>{@link #SUCCEEDED}：业务已生效（终态，任何重复投递都不得再改变业务状态）；</li>
 *   <li>{@link #DEAD}：超过重试上限或不可重试，已进入死信（终态）。</li>
 * </ul>
 */
public enum MessageState {
    PENDING,
    IN_FLIGHT,
    RETRY_WAIT,
    SUCCEEDED,
    DEAD;

    /** 是否为不可再变更的终态。 */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD;
    }
}
