package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Delivery;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.IgnoreReason;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageStatus;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.FileStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 服务重启恢复测试：使用 {@link FileStateStore} 把状态落到本地 JSON 文件，
 * 关闭 broker 后用同一文件新建实例，模拟进程重启。
 *
 * <p>覆盖：已成功消息重启后不重复生效、位点持久化、重启前在途未确认的消息被重新投递，
 * 且旧投递句柄失效（UNKNOWN_DELIVERY），业务最终仍只生效一次；退避中的消息重启后保留退避语义。
 */
class RestartRecoveryTest {

    /** 模拟外部业务存储：跨 broker 实例（跨“重启”）累计业务生效次数。 */
    private static final AtomicInteger SHARED_EFFECTS = new AtomicInteger();

    private RetryPolicy policy() {
        return RetryPolicy.of(3, Duration.ofMinutes(1), 2.0, Duration.ofMinutes(10),
                Duration.ofMinutes(5));
    }

    @Test
    void inflightMessageIsRedeliveredAfterRestartWithoutDoubleEffect(@TempDir Path dir) {
        TestSupport.title("restart recovery: inflight redelivery");
        Path file = dir.resolve("broker-state.json");
        String group = "orders";
        String topic = "orders";

        // 模拟真实外部业务存储的生效次数：跨“进程重启”共享
        AtomicInteger applied = SHARED_EFFECTS;
        applied.set(0);

        // ---- 第一个进程：入队 2 条，消费成功第 0 条，第 1 条停留在在途未确认 ----
        IdempotentBroker b1 = new IdempotentBroker(new FileStateStore(file), policy());
        b1.subscribe(group, (id, p) -> {
            applied.incrementAndGet();
            return ConsumeOutcome.success();
        });
        b1.enqueue(topic, "m0", "a");
        b1.enqueue(topic, "m1", "b");

        // 先完整消费 m0（poll->回调->成功确认）
        var firstConsumed = b1.consumeOnce(group, topic, "old-consumer", 1);
        assertEquals(1, firstConsumed.size());
        assertEquals(DeliveryOutcome.ACK_ACCEPTED, firstConsumed.get(0).outcome());
        // 再把 m1 拉下来但不确认，模拟消费者宕机
        var firstPoll = b1.poll(group, topic, "old-consumer", 10);
        TestSupport.logDeliveries("b1 poll (m1 left inflight)", firstPoll);
        assertEquals(1, firstPoll.size());
        assertEquals("m1", firstPoll.get(0).messageId());
        // 提交位点到 1 并落盘
        var commit = b1.commitOffset(group, topic, 1);
        TestSupport.logOffset(group, topic, commit);
        assertTrue(commit.accepted());
        Delivery inflightM1 = firstPoll.get(0);
        System.out.println("[RESTART] m1 left inflight with delivery=" + inflightM1.deliveryId());
        b1.close();

        // ---- 模拟重启：新 broker 指向同一状态文件 ----
        IdempotentBroker b2 = new IdempotentBroker(new FileStateStore(file), policy());
        b2.subscribe(group, (id, p) -> {
            applied.incrementAndGet();
            return ConsumeOutcome.success();
        });

        // 位点持久化
        assertEquals(1L, b2.committedOffset(group, topic));
        System.out.println("[RESTART] committed offset restored = 1");

        // 旧句柄在重启后失效：用它确认必须 UNKNOWN_DELIVERY
        var stale = b2.ack(group, inflightM1.deliveryId(), ConsumeOutcome.success());
        System.out.println("[DECIDE] stale pre-restart delivery -> " + stale.outcome()
                + " reason=" + stale.ignoreReason());
        assertEquals(DeliveryOutcome.DUPLICATE_IGNORED, stale.outcome());
        assertEquals(IgnoreReason.UNKNOWN_DELIVERY, stale.ignoreReason());

        // m0 已成功：不会重投；m1 在途租约在重启时被恢复：由新进程完整消费（poll->回调->确认）
        var consumedAfter = b2.consumeOnce(group, topic, "new-consumer", 10);
        System.out.println("[DELIVER b2 after restart] consumed=" + consumedAfter.size());
        assertEquals(1, consumedAfter.size());
        var redelivery = consumedAfter.get(0);
        assertEquals("m1", redelivery.record().messageId());
        assertEquals(1, redelivery.attempt(), "restart redelivery does not count as a failed retry");
        assertEquals(DeliveryOutcome.ACK_ACCEPTED, redelivery.outcome());
        var recs = b2.records(group, topic);
        assertEquals(MessageStatus.SUCCEEDED, recs.get(0).status());
        assertEquals(MessageStatus.SUCCEEDED, recs.get(1).status());
        assertEquals(1, recs.get(0).businessEffectCount());
        assertEquals(1, recs.get(1).businessEffectCount());
        assertEquals(2, applied.get(), "business applied once per message across restart");

        var commit2 = b2.commitOffset(group, topic, 2);
        TestSupport.logOffset(group, topic, commit2);
        assertTrue(commit2.accepted());

        // 再次重启：什么都不应再投递，位点仍为 2
        b2.close();
        IdempotentBroker b3 = new IdempotentBroker(new FileStateStore(file), policy());
        assertEquals(0, b3.poll(group, topic, "x", 10).size());
        assertEquals(2L, b3.committedOffset(group, topic));
        TestSupport.logRecord(b3, group, topic);
        b3.close();
    }

    @Test
    void deadLetterAndRetryStateSurviveRestart(@TempDir Path dir) {
        TestSupport.title("restart recovery: DLQ + retry state survive");
        Path file = dir.resolve("broker-state-2.json");
        String group = "g";
        String topic = "t";

        // m-dead：不可重试直接死信；m-retry：一次可重试失败，进入退避（1 分钟）
        IdempotentBroker b1 = new IdempotentBroker(new FileStateStore(file), policy());
        b1.enqueue(topic, "m-dead", "x");
        b1.enqueue(topic, "m-retry", "y");

        var dl = b1.poll(group, topic, "c", 10);
        Delivery deadD = dl.stream().filter(d -> d.messageId().equals("m-dead")).findFirst().orElseThrow();
        Delivery retryD = dl.stream().filter(d -> d.messageId().equals("m-retry")).findFirst().orElseThrow();
        b1.ack(group, deadD.deliveryId(), ConsumeOutcome.nonRetryable("bad"));
        b1.ack(group, retryD.deliveryId(), ConsumeOutcome.retryable("flaky"));
        b1.close();

        IdempotentBroker b2 = new IdempotentBroker(new FileStateStore(file), policy());
        // 退避未到期：m-retry 不投递；m-dead 已死信：不投递
        List<Delivery> polled = b2.poll(group, topic, "c", 10);
        TestSupport.logDeliveries("b2 poll (backoff not due)", polled);
        assertEquals(0, polled.size());

        var records = b2.records(group, topic);
        var dead = records.stream().filter(r -> r.messageId().equals("m-dead")).findFirst().orElseThrow();
        var retry = records.stream().filter(r -> r.messageId().equals("m-retry")).findFirst().orElseThrow();
        assertEquals(MessageStatus.DEAD, dead.status());
        assertEquals(MessageStatus.RETRYING, retry.status());
        assertEquals(1, dead.attemptCount());
        assertEquals(1, retry.attemptCount());
        assertEquals(1, b2.deadLetters(group).size());
        System.out.println("[RESTART] dead retained=" + dead.terminalReason()
                + ", retry nextVisibleAt=" + retry.nextVisibleAt());
        b2.close();
    }
}
