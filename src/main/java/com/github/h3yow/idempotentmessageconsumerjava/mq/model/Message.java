package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 一条本地消息。消息一旦构造即不可变；消息 id 同时承担业务幂等键的角色：
 * 同一 id 重复入队会被识别并忽略，同一消费者组内同一 id 只允许生效一次。
 *
 * @param id         消息唯一标识（也是业务去重键）
 * @param topic      主题（逻辑队列名）
 * @param payload    消息内容（字符串形式，测试中可放可读文本）
 * @param enqueueAt  首次入队时间（epoch millis）
 */
public record Message(String id, String topic, String payload, long enqueueAt) {
}
