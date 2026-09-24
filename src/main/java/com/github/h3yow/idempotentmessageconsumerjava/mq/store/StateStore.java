package com.github.h3yow.idempotentmessageconsumerjava.mq.store;

/** 状态存储抽象。 */
public interface StateStore {
    BrokerState load();
    void save(BrokerState state);
}
