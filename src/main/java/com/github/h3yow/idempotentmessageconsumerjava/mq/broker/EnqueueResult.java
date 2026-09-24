package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;

/**
 * 入队结论。重复入队不会报错，而是以可区分的原因返回，调用方可据此解释“为何没生效”。
 *
 * @param accepted true 表示新消息被接受；false 表示因重复 id 被忽略
 * @param code     {@link DecisionCode#ENQUEUED} 或 {@link DecisionCode#ENQUEUE_DUPLICATED_GLOBAL_ID}
 * @param message  被接受时为新消息；重复时为首次入队的那条原始消息
 */
public record EnqueueResult(boolean accepted, DecisionCode code, Message message) {
}
