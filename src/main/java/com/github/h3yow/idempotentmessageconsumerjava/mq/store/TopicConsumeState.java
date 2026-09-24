package com.github.h3yow.idempotentmessageconsumerjava.mq.store;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageRecord;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一个消费者组在一个 topic 上的消费状态：
 * 已提交位点，以及按 messageId 索引的处理记录（保留全部消息以支撑幂等与死信查询）。
 */
public final class TopicConsumeState {

    /** 已提交位点：所有 offset &lt; committedOffset 的消息均已连续确认。初始为 0。 */
    private long committedOffset;

    /** messageId -> 处理记录，插入顺序即位点顺序。 */
    private final Map<String, MessageRecord> records = new LinkedHashMap<>();

    public TopicConsumeState() {
    }

    public long committedOffset() {
        return committedOffset;
    }

    public void committedOffset(long value) {
        this.committedOffset = value;
    }

    public Map<String, MessageRecord> records() {
        return records;
    }

    public MessageRecord record(String messageId) {
        return records.get(messageId);
    }

    public void putRecord(MessageRecord record) {
        records.put(record.messageId(), record);
    }
}
