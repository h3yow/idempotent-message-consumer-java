package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.config.BrokerProperties;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证 localmq 自动装配：broker/properties Bean 存在，端到端一条消息可消费、位点推进。
 *
 * <p>状态文件固定在 {@code target/smoke-state.json}（刻意不清理），用于同时验证
 * “同一状态目录反复跑结果一致”：断言全部相对于运行前的位点，重复执行时
 * 历史消息保持终态不重投，新消息恰好推进一位。</p>
 */
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

        // 相对断言：同一状态文件重复运行时，历史消息的终态与位点原样保留
        long committedBefore = broker.getCommittedOffset("smoke-group");

        String id = UUID.randomUUID().toString();
        broker.enqueue(new Message(id, "smoke", "hello", System.currentTimeMillis()));
        LeasedMessage lease = broker.poll("smoke-group", "smoke-c");
        assertNotNull(lease, "新入队的消息必须可投递（历史消息均为终态，不会挡路）");
        assertEquals(id, lease.message().id(), "重复运行时历史消息不得重投，应恰好拿到新消息");
        broker.complete(lease, ConsumeOutcome.success(), null);

        System.out.println("【输入】新消息 id=" + id + " offset=" + lease.offset());
        System.out.println("【位点】运行前 committed=" + committedBefore
                + " 运行后 committed=" + broker.getCommittedOffset("smoke-group"));

        assertEquals(committedBefore + 1, broker.getCommittedOffset("smoke-group"),
                "每次运行恰好消费一条新消息，位点恰好推进一位");
        assertNull(broker.poll("smoke-group", "smoke-c"), "消费完后不应再有可投递消息");
    }
}
