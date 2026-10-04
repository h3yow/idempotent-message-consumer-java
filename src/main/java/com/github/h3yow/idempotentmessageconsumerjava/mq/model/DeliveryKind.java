package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 一次投递的来源分类，用于区分“正常投递”与“运维操作触发的重放投递”。
 *
 * <ul>
 *   <li>{@link #NORMAL}：首次投递或自动重试；</li>
 *   <li>{@link #DEAD_LETTER_REPLAY}：人工死信重投触发的投递（同一条消息的延续，非新消息）；</li>
 *   <li>{@link #OFFSET_REWIND}：消费进度回退后，对历史位点的重放投递。</li>
 * </ul>
 */
public enum DeliveryKind {
    NORMAL,
    DEAD_LETTER_REPLAY,
    OFFSET_REWIND
}
