package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.GroupSnapshot;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地持久化边界。内存态 broker 在每次关键状态变更后调用 {@link #persist(BrokerState)}，
 * 启动时调用 {@link #load()} 恢复，从而保证重启后未确认消息可重新投递、成功状态不丢不重。
 */
public interface BrokerStore {

    /**
     * 全量持久化状态。实现需保证原子落盘（先写临时文件再 rename），避免半写入文件污染重启恢复。
     */
    void persist(BrokerState state);

    /**
     * 加载上次持久化的状态；首次启动（无文件）时返回空状态，不得抛异常。
     */
    BrokerState load();

    /** broker 的全量状态聚合体。 */
    final class BrokerState {
        /** 全局消息表（保持入队顺序），key 为 messageId。 */
        private final Map<String, Message> messages = new LinkedHashMap<>();
        /** 每个消费者组的位点与每条消息的处理轨迹。 */
        private final Map<String, GroupSnapshot> groups = new LinkedHashMap<>();
        /** 死信。 */
        private final List<DeadLetter> deadLetters = new ArrayList<>();

        public Map<String, Message> getMessages() {
            return messages;
        }

        public Map<String, GroupSnapshot> getGroups() {
            return groups;
        }

        public List<DeadLetter> getDeadLetters() {
            return deadLetters;
        }
    }
}
