package com.github.h3yow.idempotentmessageconsumerjava.mq.store;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageEnvelope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * broker 的全部可持久化状态。
 *
 * <ul>
 *   <li>{@code logs}：topic -> 有序消息日志（按位点）；</li>
 *   <li>{@code groups}：消费者组 -> 组状态（位点 + 处理记录）；</li>
 *   <li>{@code published}：messageId -> topic，入队幂等去重表；</li>
 *   <li>{@code seq}：messageId 自增序号。</li>
 * </ul>
 */
public final class BrokerState {

    private final Map<String, List<MessageEnvelope>> logs = new LinkedHashMap<>();
    private final Map<String, GroupState> groups = new LinkedHashMap<>();
    private final Map<String, String> published = new HashMap<>();
    private long seq;

    public BrokerState() {
    }

    public List<MessageEnvelope> log(String topic) {
        return logs.computeIfAbsent(topic, t -> new ArrayList<>());
    }

    public Map<String, List<MessageEnvelope>> logs() {
        return logs;
    }

    public GroupState group(String group) {
        return groups.computeIfAbsent(group, g -> new GroupState());
    }

    public Map<String, GroupState> groups() {
        return groups;
    }

    public Map<String, String> published() {
        return published;
    }

    public long nextSeq() {
        return ++seq;
    }

    public long seq() {
        return seq;
    }

    public void seq(long value) {
        this.seq = value;
    }
}
