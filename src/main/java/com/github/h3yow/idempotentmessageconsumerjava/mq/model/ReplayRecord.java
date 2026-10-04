package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 一次运维重放动作（死信重投 / 进度回退）作用于某条消息的持久化审计记录。
 *
 * @param replayNo   该消息（在组内）第几次被人工/运维重放，从 1 开始
 * @param kind        本次重放来源：DEAD_LETTER_REPLAY / OFFSET_REWIND
 * @param operator    操作人/操作来源标识（可为 null，默认 SYSTEM）
 * @param reason      操作原因（人工补偿说明）
 * @param offset      消息位点
 * @param messageId   消息 id
 * @param createdAt   记录产生时间（epoch millis）
 */
public record ReplayRecord(int replayNo,
                           DeliveryKind kind,
                           String operator,
                           String reason,
                           long offset,
                           String messageId,
                           long createdAt) {
}
