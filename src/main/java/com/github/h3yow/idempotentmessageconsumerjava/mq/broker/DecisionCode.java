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

    /* ---- 死信重投（人工补偿） ---- */
    REDELIVERED_FROM_DEAD_LETTER,   // 死信被人工重投，重新进入待投递（原死信记录保留）
    REDELIVER_REJECTED_UNKNOWN_MESSAGE, // 重投被拒绝：组内不存在该消息
    REDELIVER_REJECTED_NOT_DEAD,    // 重投被拒绝：消息当前不是死信终态
    REDELIVER_DISPATCHED,           // 被人工重投的消息再次投递（与自动重试可区分）

    /* ---- 消费进度重放 ---- */
    REPLAY_SCHEDULED,               // 进度回退被接受，已处理消息被重置为待重放
    REPLAY_REJECTED_NOT_BACKWARD,   // 重放被拒绝：目标位点不是回退（>= 当前已提交位点）
    REPLAY_REJECTED_RANGE,          // 重放被拒绝：目标位点越界
    REPLAY_DISPATCHED,              // 因重放而再次投递（与新消息可区分）

    /* ---- 重启恢复 ---- */
    RECOVERED_ON_STARTUP,           // 启动时加载快照，未确认租约重置为待投递
    PERSISTED                       // 状态已持久化
}
