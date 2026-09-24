package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import java.util.List;

/**
 * 死信记录：消息达到重试上限或被判定不可重试后产生。
 * 原始消息内容与全部尝试记录一并保留，不丢失原始记录。
 *
 * @param message         原始消息
 * @param group           所属消费者组
 * @param attempts        全部尝试记录（按时间顺序）
 * @param deadAt          进入死信的时间（epoch millis）
 * @param cause           进入死信的直接原因：MAX_ATTEMPTS_REACHED / NON_RETRYABLE_BY_HANDLER
 */
public record DeadLetter(Message message,
                         String group,
                         List<AttemptRecord> attempts,
                         long deadAt,
                         String cause) {
}
