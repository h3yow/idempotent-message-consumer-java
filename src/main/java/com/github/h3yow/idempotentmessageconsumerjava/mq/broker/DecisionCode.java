package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

/**
 * 中间件每一次关键判定的编码，写入审计轨迹，使“忽略原因 / 失败原因 / 拒绝原因”均可区分。
 */
public enum DecisionCode {

    /* ---- 入队 ---- */
    ENQUEUED,                       // 新消息成功入队
    ENQUEUE_DUPLICATED_GLOBAL_ID,   // 相同 messageId 已存在（全局），重复入队被忽略

    /* ---- 投递 / 租约 ---- */
    DISPATCHED,                     // 正常首次/重试投递，已发放租约
    DISPATCH_SKIP_BELOW_COMMITTED,  // 位点 <= committedOffset，按协议不可能再投递
    DISPATCH_SKIP_INFLIGHT,         // 消息仍有有效租约，不能重复投递
    DISPATCH_SKIP_WAITING_BACKOFF,  // 退避窗口未到，暂不投递
    DISPATCH_SKIP_TERMINAL,         // 消息已 SUCCEEDED/DEAD，重复投递被忽略
    LEASE_TIMED_OUT,                // 租约超时，回收并安排重新投递
    LEASE_STALE_IGNORED,            // 迟到/错误的 leaseId 确认，租约已失效，忽略

    /* ---- 幂等忽略（消费侧） ---- */
    DUPLICATE_ALREADY_SUCCEEDED,    // 重复消费：该消息此前已成功生效，业务不再次执行

    /* ---- 处理结果 ---- */
    ACK_SUCCEEDED,                  // 确认成功，业务生效
    NACK_RETRY_SCHEDULED,           // 可重试失败，已安排退避后重试
    NACK_RETRY_EXHAUSTED,           // 重试次数耗尽，转入死信
    NACK_NON_RETRYABLE,             // 处理器判定不可重试，直接转入死信

    /* ---- 位点 ---- */
    OFFSET_COMMITTED,               // 位点提交成功
    OFFSET_COMMIT_REJECTED_ROLLBACK,// 目标位点小于当前已提交位点（回退被拒绝）
    OFFSET_COMMIT_REJECTED_GAP,     // 目标区间内仍有未确认消息（跨未确认提交被拒绝）
    OFFSET_COMMIT_REJECTED_RANGE,   // 目标位点超出当前最大位点

    /* ---- 重启恢复 ---- */
    RECOVERED_ON_STARTUP,           // 启动时加载快照，未确认租约重置为待投递

    /* ---- 运维：死信重投（人工补偿） ---- */
    DEAD_LETTER_REPLAY_ACCEPTED,    // 死信重投被接受，消息作为同一消息的延续重新可投递
    REPLAY_REJECTED_NOT_DEAD,       // 目标消息不是 DEAD 终态，不能死信重投
    REPLAY_REJECTED_OFFSET_MISSING, // 死信重投目标位点不存在
    REPLAY_DISPATCHED,              // 死信重投后的投递（attemptNo 延续历史，非新消息）

    /* ---- 运维：消费进度回退重放 ---- */
    OFFSET_REWIND_ACCEPTED,         // 消费进度回退被接受，历史位点重新可投递
    REWIND_REJECTED_RANGE,          // 回退目标位点 < 0 或超出最大位点
    REWIND_REJECTED_NOT_BEHIND,     // 回退目标不早于当前已提交位点（没有更早的位置可退）
    REWIND_REJECTED_INFLIGHT,       // 回退区间内存在尚未确认（IN_FLIGHT）的消息
    REPLAY_DELIVERED,               // 进度回退后的重放投递（非新消息）
    DUPLICATE_EFFECT_SUPPRESSED,    // 重放的消息此前已成功生效，本次业务效果被幂等抑制（只留痕）

    PERSISTED                       // 状态已持久化
}
