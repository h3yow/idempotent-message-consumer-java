package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Delivery;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageStatus;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 失败重试与死信测试。
 * 覆盖：可重试失败按指数退避延迟（未到退避点不投递）、attemptNo 递增、
 * 重试耗尽进入死信且原始记录/尝试全部保留；不可重试失败直接进入死信。
 */
class RetryAndDeadLetterTest {

    /** 可手动拨动的时钟，便于确定性地验证退避。 */
    static final class MutableClock extends Clock {
        private Instant t = Instant.parse("2026-09-24T00:00:00Z");

        void advance(Duration d) {
            t = t.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return t;
        }
    }

    @Test
    void retryableFailuresBackOffAndEventuallyDeadLetter() {
        TestSupport.title("retry backoff then exhausted -> DLQ");
        MutableClock clock = new MutableClock();
        // 最多 3 次尝试（首次+2 次重试）；退避 10ms,20ms；租约 1 分钟
        RetryPolicy policy = RetryPolicy.of(3, Duration.ofMillis(10), 2.0,
                Duration.ofMillis(100), Duration.ofMinutes(1));
        IdempotentBroker broker = new IdempotentBroker(new InMemoryStateStore(), policy, clock);
        String group = "payment";
        String topic = "payments";

        AtomicInteger calls = new AtomicInteger();
        broker.subscribe(group, (id, p) ->
                ConsumeOutcome.retryable("downstream timeout #" + calls.incrementAndGet()));

        broker.enqueue(topic, "pay-1", "charge");

        // 第 1 次：投递并可重试失败 -> RETRY_SCHEDULED，退避到 +10ms
        var d1 = broker.poll(group, topic, "c1", 10);
        TestSupport.logDeliveries("attempt1", d1);
        var a1 = broker.ack(group, d1.get(0).deliveryId(),
                ConsumeOutcome.retryable("downstream timeout"));
        System.out.println("[DECIDE] attempt1 -> " + a1.outcome() + " attempt=" + a1.attempt());
        assertEquals(DeliveryOutcome.RETRY_SCHEDULED, a1.outcome());

        // 未到退避点：不可投递
        clock.advance(Duration.ofMillis(5));
        assertEquals(0, broker.poll(group, topic, "c1", 10).size(),
                "message must stay hidden during backoff");
        System.out.println("[CLOCK ] +5ms, still backoff -> poll empty");

        // 到退避点：第 2 次投递，attemptNo=2，再失败，退避到 +20ms
        clock.advance(Duration.ofMillis(5));
        var d2 = broker.poll(group, topic, "c1", 10);
        TestSupport.logDeliveries("attempt2 at +10ms", d2);
        assertEquals(1, d2.size());
        assertEquals(2, d2.get(0).attemptNo());
        var a2 = broker.ack(group, d2.get(0).deliveryId(),
                ConsumeOutcome.retryable("downstream timeout"));
        System.out.println("[DECIDE] attempt2 -> " + a2.outcome());
        assertEquals(DeliveryOutcome.RETRY_SCHEDULED, a2.outcome());

        // 旧句柄在重试期间迟到确认：必须被忽略（UNKNOWN_DELIVERY），不影响重试
        var stale = broker.ack(group, d1.get(0).deliveryId(), ConsumeOutcome.success());
        System.out.println("[DECIDE] stale delivery ack -> " + stale.outcome()
                + " reason=" + stale.ignoreReason());
        assertEquals(DeliveryOutcome.DUPLICATE_IGNORED, stale.outcome());

        // 推进 20ms 过第二次退避，第 3 次投递仍失败 -> 达到上限进入死信
        clock.advance(Duration.ofMillis(20));
        var d3 = broker.poll(group, topic, "c1", 10);
        TestSupport.logDeliveries("attempt3 at +30ms", d3);
        assertEquals(3, d3.get(0).attemptNo());
        var a3 = broker.ack(group, d3.get(0).deliveryId(),
                ConsumeOutcome.retryable("downstream timeout"));
        System.out.println("[DECIDE] attempt3 -> " + a3.outcome());
        assertEquals(DeliveryOutcome.DEAD_LETTERED, a3.outcome());

        var rec = broker.records(group, topic).get(0);
        assertEquals(MessageStatus.DEAD, rec.status());
        assertEquals(3, rec.attemptCount(), "3 attempts retained");
        assertEquals(0, rec.businessEffectCount(), "never took business effect");
        assertNotNull(rec.completedAt());
        assertTrue(rec.terminalReason().contains("retry budget exhausted"),
                "terminal reason must explain exhaustion: " + rec.terminalReason());

        // 死信查询返回同一条原始记录
        var dlq = broker.deadLetters(group);
        assertEquals(1, dlq.size());
        assertEquals("pay-1", dlq.get(0).messageId());
        System.out.println("[DLQ   ] size=" + dlq.size() + " reason=" + rec.terminalReason());

        // 死信后再 poll 不再投递；死信句柄重放给出 MESSAGE_IN_DLQ
        assertEquals(0, broker.poll(group, topic, "c1", 10).size());
        var dlqAck = broker.ack(group, d3.get(0).deliveryId(), ConsumeOutcome.success());
        assertEquals(DeliveryOutcome.DUPLICATE_IGNORED, dlqAck.outcome());
        System.out.println("[DECIDE] dead delivery replay -> " + dlqAck.outcome()
                + " reason=" + dlqAck.ignoreReason());

        TestSupport.logRecord(broker, group, topic);
        broker.close();
    }

    @Test
    void nonRetryableFailureGoesStraightToDlq() {
        TestSupport.title("non-retryable failure -> DLQ immediately");
        MutableClock clock = new MutableClock();
        RetryPolicy policy = RetryPolicy.of(5, Duration.ofMillis(10), 2.0,
                Duration.ofMillis(100), Duration.ofMinutes(1));
        IdempotentBroker broker = new IdempotentBroker(new InMemoryStateStore(), policy, clock);
        String group = "risk";
        String topic = "checks";

        broker.subscribe(group, (id, p) -> ConsumeOutcome.nonRetryable("poison message: bad signature"));
        broker.enqueue(topic, "chk-1", "{}");

        List<Delivery> d = broker.poll(group, topic, "c", 10);
        var r = broker.ack(group, d.get(0).deliveryId(),
                ConsumeOutcome.nonRetryable("poison message: bad signature"));
        System.out.println("[DECIDE] -> " + r.outcome() + " attempt=" + r.attempt());
        assertEquals(DeliveryOutcome.DEAD_LETTERED, r.outcome());
        assertEquals(1, r.attempt(), "no retries for non-retryable");
        var rec = broker.records(group, topic).get(0);
        assertEquals(MessageStatus.DEAD, rec.status());
        assertTrue(rec.terminalReason().contains("non-retryable"), rec.terminalReason());
        System.out.println("[DLQ   ] reason=" + rec.terminalReason());
        TestSupport.logRecord(broker, group, topic);
        broker.close();
    }
}
