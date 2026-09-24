package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Delivery;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.IgnoreReason;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageStatus;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 重复投递 / 重复消费幂等测试。
 * 覆盖：同组内业务仅生效一次、成功后旧句柄重复确认给出 ALREADY_SUCCEEDED、
 * 成功后再次 poll 不再投递、以及入端相同 messageId 重复入队被识别为 duplicate。
 */
class DuplicateDeliveryTest {

    private IdempotentBroker newBroker() {
        return new IdempotentBroker(new InMemoryStateStore(),
                RetryPolicy.of(3, Duration.ofMillis(10), 2.0, Duration.ofMillis(100),
                        Duration.ofMinutes(1)));
    }

    @Test
    void sameMessageTakesEffectOnceAndDuplicateAckHasDistinctReason() {
        TestSupport.title("duplicate-delivery / idempotency");
        IdempotentBroker broker = newBroker();
        String group = "order-service";
        String topic = "orders";

        AtomicInteger invocationCount = new AtomicInteger();
        broker.subscribe(group, (id, payload) -> {
            invocationCount.incrementAndGet();
            return ConsumeOutcome.success();
        });

        // 同 messageId 入队两次：第二次必须被判为重复，不产生第二条日志
        var first = broker.enqueue(topic, "biz-id-1001", "create-order-1001");
        var second = broker.enqueue(topic, "biz-id-1001", "create-order-1001");
        System.out.println("[INPUT ] enqueue#1 " + first);
        System.out.println("[INPUT ] enqueue#2 " + second);
        assertEquals(0, first.offset());
        assertEquals(false, first.duplicate());
        assertEquals(true, second.duplicate());
        assertEquals(0, second.offset());

        // 完整消费一次（poll -> 业务回调 -> 成功 ack）
        TestSupport.logInput(group, topic, "consumeOnce#1", "max=10");
        var results = broker.consumeOnce(group, topic, "consumer-A", 10);
        results.forEach(r -> System.out.println("[DECIDE] => " + r.outcome()
                + ", status=" + (r.record() == null ? null : r.record().status())));
        assertEquals(1, results.size());
        assertEquals(DeliveryOutcome.ACK_ACCEPTED, results.get(0).outcome());
        assertEquals(1, invocationCount.get());

        // 记录下成功投递的句柄（已被消费），再重放它：必须忽略且原因可区分
        String consumedDelivery = results.get(0).record().attempts().get(0).deliveryId();
        var dupAck = broker.ack(group, consumedDelivery, ConsumeOutcome.success());
        System.out.println("[DECIDE] ack(replay consumed delivery) => outcome=" + dupAck.outcome()
                + ", ignoreReason=" + dupAck.ignoreReason());
        assertEquals(DeliveryOutcome.DUPLICATE_IGNORED, dupAck.outcome());
        assertEquals(IgnoreReason.ALREADY_SUCCEEDED, dupAck.ignoreReason());

        // 成功后再次 poll：不应再投递，业务回调也不会再被调用
        List<Delivery> d2 = broker.poll(group, topic, "consumer-B", 10);
        TestSupport.logDeliveries("poll after success", d2);
        assertEquals(0, d2.size());

        // 最终状态：SUCCEEDED，业务生效恰好一次，尝试记录只有 1 条，回调只被调用一次
        var rec = broker.records(group, topic).get(0);
        assertEquals(MessageStatus.SUCCEEDED, rec.status());
        assertEquals(1, rec.businessEffectCount());
        assertEquals(1, rec.attemptCount());
        assertEquals(1, invocationCount.get());

        TestSupport.logRecord(broker, group, topic);
        broker.close();
    }

    @Test
    void inflightMessageCannotBeGrabbedByAnotherConsumer() {
        TestSupport.title("inflight lease excludes other consumers");
        IdempotentBroker broker = newBroker();
        String group = "inventory";
        String topic = "stock";

        AtomicInteger invocations = new AtomicInteger();
        broker.subscribe(group, (id, p) -> {
            invocations.incrementAndGet();
            return ConsumeOutcome.success();
        });
        broker.enqueue(topic, "sku-1", "deduct");

        TestSupport.logInput(group, topic, "poll consumer-X", "msg=sku-1");
        List<Delivery> x = broker.poll(group, topic, "consumer-X", 10);
        TestSupport.logDeliveries("consumer-X", x);
        assertEquals(1, x.size());

        // 租约未到期，另一个消费者并发 poll：拿不到这条在途消息
        TestSupport.logInput(group, topic, "poll consumer-Y (concurrent)", "msg=sku-1");
        List<Delivery> y = broker.poll(group, topic, "consumer-Y", 10);
        TestSupport.logDeliveries("consumer-Y while leased", y);
        assertEquals(0, y.size());

        // 由持有者 X 完成消费，业务只生效一次
        var done = broker.consumeOnce(group, topic, "consumer-Z", 10);
        assertEquals(0, done.size(), "message still leased to X, Z cannot get it");
        var ack = broker.ack(group, x.get(0).deliveryId(), ConsumeOutcome.success());
        assertEquals(DeliveryOutcome.ACK_ACCEPTED, ack.outcome());
        assertEquals(1, broker.records(group, topic).get(0).businessEffectCount());
        assertEquals(0, invocations.get(), "manual ack does not re-run business; consumeOnce got nothing");
        TestSupport.logRecord(broker, group, topic);
        broker.close();
    }
}
