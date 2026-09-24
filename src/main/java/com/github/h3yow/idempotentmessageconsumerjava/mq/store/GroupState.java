package com.github.h3yow.idempotentmessageconsumerjava.mq.store;

import java.util.HashMap;
import java.util.Map;

/**
 * 一个消费者组的状态：topic -> 该组在该 topic 上的消费状态。
 */
public final class GroupState {

    private final Map<String, TopicConsumeState> topics = new HashMap<>();

    public GroupState() {
    }

    public TopicConsumeState topic(String topic) {
        return topics.computeIfAbsent(topic, t -> new TopicConsumeState());
    }

    public Map<String, TopicConsumeState> topics() {
        return topics;
    }
}
