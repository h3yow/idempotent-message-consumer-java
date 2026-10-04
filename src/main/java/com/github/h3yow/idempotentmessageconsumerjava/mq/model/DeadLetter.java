package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import java.util.List;

/**
 * 死信记录：消息达到重试上限或被判定不可重试后产生。
 * 原始消息内容与全部尝试记录一并保留，不丢失原始记录。
 *
 * <p>人工死信重投不会抹掉任何已有的死信记录：每次重放后再次失败进入死信都会新增一条
 * {@code replayNo >= 1} 的记录；{@code replayNo == 0} 表示该消息第一次（自动重试耗尽/
 * 不可重试）进入死信。</p>
 *
 * @param message         原始消息
 * @param group           所属消费者组
 * @param attempts        截至本次死信时的全部尝试记录（按时间顺序）
 * @param deadAt          进入死信的时间（epoch millis）
 * @param cause           进入死信的直接原因：MAX_ATTEMPTS_REACHED / NON_RETRYABLE_BY_HANDLER
 * @param replayNo        第几次重放之后再次死信；0 表示从未被重放过的原始死信
 * @param operator        若死信源于人工重投后的再次失败，记录操作人；否则为 null
 */
public record DeadLetter(Message message,
                         String group,
                         List<AttemptRecord> attempts,
                         long deadAt,
                         String cause,
                         int replayNo,
                         String operator) {

    /** 首次进入死信（非重放）的构造，replayNo=0。 */
    public DeadLetter(Message message, String group, List<AttemptRecord> attempts,
                      long deadAt, String cause) {
        this(message, group, attempts, deadAt, cause, 0, null);
    }
}
