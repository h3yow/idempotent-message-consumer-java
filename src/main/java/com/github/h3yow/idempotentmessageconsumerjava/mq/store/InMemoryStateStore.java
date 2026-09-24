package com.github.h3yow.idempotentmessageconsumerjava.mq.store;

import java.util.function.Supplier;

/**
 * 纯内存存储：broker 生命周期内保存全部状态，关闭即丢失。
 * 用于不要求重启恢复的测试场景。
 */
public final class InMemoryStateStore implements StateStore {

    private BrokerState state;
    private final Supplier<BrokerState> factory;

    public InMemoryStateStore() {
        this(BrokerState::new);
    }

    public InMemoryStateStore(Supplier<BrokerState> factory) {
        this.factory = factory;
    }

    @Override
    public synchronized BrokerState load() {
        if (state == null) {
            state = factory.get();
        }
        return state;
    }

    @Override
    public synchronized void save(BrokerState state) {
        this.state = state;
    }
}
