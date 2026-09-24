package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 一次消费尝试的完整审计记录，保证“每次尝试的原因与结果可追溯”。
 *
 * @param attemptNo      第几次尝试（从 1 开始）
 * @param consumerId     执行本次尝试的消费者标识
 * @param startedAt      尝试开始时间（epoch millis）
 * @param finishedAt     尝试结束时间（epoch millis）
 * @param result         尝试结果
 * @param reason         触发本次尝试或得到该结果的原因（可区分：HANDLER_INVOKED /
 *                       LEASE_TIMEOUT_REDELIVER / DUPLICATE_ALREADY_SUCCEEDED 等）
 * @param detail         附加说明（如异常类型与消息），可为 null
 */
public record AttemptRecord(int attemptNo,
                            String consumerId,
                            long startedAt,
                            long finishedAt,
                            AttemptResult result,
                            String reason,
                            String detail) {
}
