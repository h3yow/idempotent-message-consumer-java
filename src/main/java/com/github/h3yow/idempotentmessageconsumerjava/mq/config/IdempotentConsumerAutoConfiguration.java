package com.github.h3yow.idempotentmessageconsumerjava.mq.config;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.FileStateStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.InMemoryStateStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.StateStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.nio.file.Path;

/**
 * 自动装配：根据 {@link IdempotentConsumerProperties} 创建 {@link StateStore}
 * 与单例 {@link IdempotentBroker}。业务方可直接注入 broker，
 * 再调用 {@code subscribe} 注册消费回调。
 */
@AutoConfiguration
@EnableConfigurationProperties(IdempotentConsumerProperties.class)
public class IdempotentConsumerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public StateStore idempotentMqStateStore(IdempotentConsumerProperties props) {
        if (props.getStateFile() == null || props.getStateFile().isBlank()) {
            return new InMemoryStateStore();
        }
        return new FileStateStore(Path.of(props.getStateFile()));
    }

    @Bean
    @ConditionalOnMissingBean
    public IdempotentBroker idempotentBroker(StateStore stateStore,
                                             IdempotentConsumerProperties props) {
        RetryPolicy policy = RetryPolicy.of(
                props.getMaxAttempts(),
                props.getBackoffInitial(),
                props.getBackoffMultiplier(),
                props.getBackoffMax(),
                props.getVisibilityLease());
        return new IdempotentBroker(stateStore, policy);
    }
}
