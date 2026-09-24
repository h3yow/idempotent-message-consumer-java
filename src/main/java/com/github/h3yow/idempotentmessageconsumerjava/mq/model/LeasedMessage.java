package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 一次租约（lease）投递的载体。消费者必须凭 leaseId 确认；
 * 租约到期未确认，消息会被判定为 {@link AttemptResult#LEASE_TIMEOUT} 并重新投递。
 *
 * @param leaseId        租约唯一标识，ack/nack 时校验，防止迟到确认污染状态
 * @param group          消费者组
 * @param offset         消息在该组分区内的位点（单调递增，从 0 开始）
 * @param message        原始消息
 * @param attemptNo      本次是第几次尝试（首次投递为 1）
 * @param grantedAt      租约发放时间（epoch millis）
 * @param leaseExpiresAt 租约到期时间（epoch millis）
 */
public record LeasedMessage(String leaseId,
                            String group,
                            long offset,
                            Message message,
                            int attemptNo,
                            long grantedAt,
                            long leaseExpiresAt) {
}
