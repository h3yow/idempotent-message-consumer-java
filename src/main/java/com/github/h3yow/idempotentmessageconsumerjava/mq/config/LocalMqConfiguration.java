package com.github.h3yow.idempotentmessageconsumerjava.mq.config;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Spring 装配：默认提供一个基于文件快照的 {@link LocalMessageBroker} 单例。
 * 消费者组与处理器由应用代码按需 register/start，组件本身不做主题扫描。
 * 可用 {@code localmq.enabled=false} 关闭。
 */
@Configuration
@EnableConfigurationProperties(BrokerProperties.class)
@ConditionalOnProperty(prefix = "localmq", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class LocalMqConfiguration {

    @Bean(destroyMethod = "flush")
    public LocalMessageBroker localMessageBroker(BrokerProperties props) {
        return new LocalMessageBroker(new FileBrokerStore(Path.of(props.getStateFile())));
    }
}
