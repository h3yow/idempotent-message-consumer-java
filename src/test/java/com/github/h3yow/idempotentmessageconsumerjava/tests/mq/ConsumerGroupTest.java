package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 按消费者组分发测试：同一条消息对每个组独立投递、独立记位点；
 * 同一组内只生效一次，不同组各自生效一次（发布-订阅语义）。
 */
class ConsumerGroupTest {

    @Test
    void messageIsDeliveredPerGroupButOnceWithinGroup() {
        TestSupport.title("consumer-group fan-out");
        IdempotentBroker broker = new IdempotentBroker(new InMemoryStateStore(),
                RetryPolicy.of(3, Duration.ofMillis(10), 2.0, Duration.ofMillis(100),
                        Duration.ofMinutes(1)));

        AtomicInteger g1 = new AtomicInteger();
        AtomicInteger g2 = new AtomicInteger();
        broker.subscribe("group-a", (id, p) -> {
            g1.incrementAndGet();
            return com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome.success();
        });
        broker.subscribe("group-b", (id, p) -> {
            g2.incrementAndGet();
            return com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome.success();
        });

        broker.enqueue("topic", "shared-1", "hello");

        var a1 = broker.consumeOnce("group-a", "topic", "a", 10);
        var b1 = broker.consumeOnce("group-b", "topic", "b", 10);
        var a2 = broker.consumeOnce("group-a", "topic", "a", 10);
        var b2 = broker.consumeOnce("group-b", "topic", "b", 10);

        System.out.println("[FANOUT] group-a first=" + a1.size() + " second=" + a2.size()
                + " effects=" + g1.get());
        System.out.println("[FANOUT] group-b first=" + b1.size() + " second=" + b2.size()
                + " effects=" + g2.get());

        assertEquals(DeliveryOutcome.ACK_ACCEPTED, a1.get(0).outcome());
        assertEquals(DeliveryOutcome.ACK_ACCEPTED, b1.get(0).outcome());
        assertEquals(0, a2.size());
        assertEquals(0, b2.size());
        assertEquals(1, g1.get());
        assertEquals(1, g2.get());

        // 位点按组独立
        var ca = broker.commitOffset("group-a", "topic", 1);
        var cb0 = broker.committedOffset("group-b", "topic");
        assertEquals(true, ca.accepted());
        assertEquals(0L, cb0, "group-b offset untouched by group-a commit");
        TestSupport.logOffset("group-a", "topic", ca);
        System.out.println("[OFFSET] group-b committed (before own commit) = " + cb0);
        broker.close();
    }
}
