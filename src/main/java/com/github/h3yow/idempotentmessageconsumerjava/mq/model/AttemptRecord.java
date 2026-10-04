package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 一次消费尝试的完整审计记录，保证“每次尝试的原因与结果可追溯”。
 *
 * @param attemptNo      第几次尝试（从 1 开始；死信重投/重放也延续同一序号序列，不当作新消息）
 * @param consumerId     执行本次尝试的消费者标识
 * @param startedAt      尝试开始时间（epoch millis）
 * @param finishedAt     尝试结束时间（epoch millis）
 * @param result         尝试结果
 * @param reason         触发本次尝试或得到该结果的原因（可区分：HANDLER_INVOKED /
 *                       LEASE_TIMEOUT_REDELIVER / RECOVERED_AFTER_RESTART_UNACKED /
 *                       DEAD_LETTER_REPLAY / OFFSET_REWIND 等）
 * @param detail         附加说明（如异常类型与消息），可为 null
 * @param deliveryKind   本次投递来源：NORMAL / DEAD_LETTER_REPLAY / OFFSET_REWIND；
 *                       历史快照缺省反序列化时归一化为 NORMAL
 */
public record AttemptRecord(int attemptNo,
                            String consumerId,
                            long startedAt,
                            long finishedAt,
                            AttemptResult result,
                            String reason,
                            String detail,
                            DeliveryKind deliveryKind) {

    public AttemptRecord {
        if (deliveryKind == null) {
            deliveryKind = DeliveryKind.NORMAL;
        }
    }

    /** 不含投递来源的便捷构造（等价于 {@link DeliveryKind#NORMAL}）。 */
    public AttemptRecord(int attemptNo,
                         String consumerId,
                         long startedAt,
                         long finishedAt,
                         AttemptResult result,
                         String reason,
                         String detail) {
        this(attemptNo, consumerId, startedAt, finishedAt, result, reason, detail,
                DeliveryKind.NORMAL);
    }
}
