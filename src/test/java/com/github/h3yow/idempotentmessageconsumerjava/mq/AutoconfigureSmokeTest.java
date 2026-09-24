package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.config.BrokerProperties;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 验证 localmq 自动装配：broker/properties Bean 存在，端到端一条消息可消费、位点推进。 */
@SpringBootTest
@TestPropertySource(properties = "localmq.state-file=target/smoke-state.json")
class AutoconfigureSmokeTest {

    @Autowired
    LocalMessageBroker broker;

    @Autowired
    BrokerProperties properties;

    @Test
    void beanWiredAndEndToEndWorks() {
        assertNotNull(broker);
        assertEquals(3, properties.getDefaultMaxAttempts());
        broker.registerGroup(ConsumerGroupConfig.of("smoke-group", "smoke"));

        String id = UUID.randomUUID().toString();
        broker.enqueue(new Message(id, "smoke", "hello", System.currentTimeMillis()));
        var lease = broker.poll("smoke-group", "smoke-c");
        assertNotNull(lease);
        broker.complete(lease, ConsumeOutcome.success(), null);

        assertEquals(0L, broker.getCommittedOffset("smoke-group"));
        assertNull(broker.poll("smoke-group", "smoke-c"));
    }
}
