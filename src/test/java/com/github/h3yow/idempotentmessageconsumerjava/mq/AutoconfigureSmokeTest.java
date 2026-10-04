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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证 localmq 自动装配：broker/properties Bean 存在，端到端一条消息可消费、位点推进。
 *
 * <p>可重复运行保证：状态文件是磁盘上的共享外部状态，若不清理，上一遍运行残留的消息会让
 * “committed=0”这类断言在第二遍失败。这里用静态初始化块在类加载（也早于 Spring 上下文
 * 初始化与 broker 加载快照）时删除残留文件，使同一套测试无论连跑多少遍初始状态都一致
 * （其他测试一律使用 {@code @TempDir}，天然隔离）。</p>
 */
@SpringBootTest
@TestPropertySource(properties = "localmq.state-file=target/smoke-state.json")
class AutoconfigureSmokeTest {

    private static final Path STATE_FILE = Path.of("target/smoke-state.json");

    static {
        try {
            Files.deleteIfExists(STATE_FILE);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

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
