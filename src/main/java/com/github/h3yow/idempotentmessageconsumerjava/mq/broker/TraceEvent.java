package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

/**
 * 一条审计轨迹事件。所有关键判定（入队、投递、忽略、ack/nack、重试、死信、位点提交、
 * 重启恢复）都会产生一条 TraceEvent，并同步打印到日志，便于测试中断言“判定依据”。
 *
 * @param seq        进程内单调递增的事件序号
 * @param timestamp  事件时间（epoch millis）
 * @param group      相关消费者组（可为 null，如全局入队事件）
 * @param messageId  相关消息 id（可为 null）
 * @param offset     相关位点（无位点上下文时为 -1）
 * @param code       判定编码
 * @param detail     判定依据的人类可读描述（包含输入值与位点等关键事实）
 */
public record TraceEvent(long seq,
                         long timestamp,
                         String group,
                         String messageId,
                         long offset,
                         DecisionCode code,
                         String detail) {
}
