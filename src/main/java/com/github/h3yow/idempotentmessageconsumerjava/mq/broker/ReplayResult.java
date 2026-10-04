package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

/**
 * 消费进度重放（回退）被接受后的结果摘要。
 *
 * @param group               目标消费者组
 * @param previousCommitted   回退前的已提交位点
 * @param committed           回退后的已提交位点（即请求的目标位点）
 * @param resetToPending      被重置为待重放的消息数（此前为 SUCCEEDED）
 * @param leftInFlight        区间内仍在租约期、未被重置的消息数（其进行中的确认不受影响）
 * @param leftDead            区间内保持死信终态的消息数（死信请用人工重投，不随重放复活）
 */
public record ReplayResult(String group,
                           long previousCommitted,
                           long committed,
                           int resetToPending,
                           int leftInFlight,
                           int leftDead) {
}
