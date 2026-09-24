package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Delivery;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.RejectReason;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.RetryPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发消费下的位点管理测试。
 * 覆盖：正常连续提交、回退提交被拒（REGRESSED）、越过未确认消息被拒（GAP_WITH_UNACKED）、
 * 越界提交被拒（UNKNOWN_MESSAGE），以及多线程并发提交/消费时数据结构不损坏。
 */
class OffsetCommitTest {

    private IdempotentBroker newBroker() {
        return new IdempotentBroker(new InMemoryStateStore(),
                RetryPolicy.of(3, Duration.ofMillis(10), 2.0, Duration.ofMillis(100),
                        Duration.ofMinutes(1)));
    }

    @Test
    void offsetRules_regressionGapAndOutOfRangeAreRejected() {
        TestSupport.title("offset commit rules");
        IdempotentBroker broker = newBroker();
        String group = "g1";
        String topic = "t1";
        broker.subscribe(group, (id, p) -> ConsumeOutcome.success());

        for (int i = 0; i < 3; i++) {
            broker.enqueue(topic, "m" + i, "p" + i);
        }

        // 初始位点 0
        assertEquals(0L, broker.committedOffset(group, topic));

        // 一条都没确认就提交到 3：必须被 GAP 拒绝
        var r0 = broker.commitOffset(group, topic, 3);
        TestSupport.logOffset(group, topic, r0);
        assertFalse(r0.accepted());
        assertEquals(RejectReason.GAP_WITH_UNACKED, r0.rejectReason());

        // 只确认第 0 条（首次只拉取 1 条），然后尝试提交到 2：因 offset=1 未确认被 GAP 拒绝
        List<Delivery> first = broker.poll(group, topic, "c", 1);
        broker.ack(group, first.get(0).deliveryId(), ConsumeOutcome.success());
        var r1 = broker.commitOffset(group, topic, 2);
        TestSupport.logOffset(group, topic, r1);
        assertFalse(r1.accepted());
        assertEquals(RejectReason.GAP_WITH_UNACKED, r1.rejectReason());

        // 提交到 1（连续前缀）：接受
        var r2 = broker.commitOffset(group, topic, 1);
        TestSupport.logOffset(group, topic, r2);
        assertTrue(r2.accepted());
        assertEquals(1L, broker.committedOffset(group, topic));

        // 回退提交到 0：REGRESSED
        var r3 = broker.commitOffset(group, topic, 0);
        TestSupport.logOffset(group, topic, r3);
        assertFalse(r3.accepted());
        assertEquals(RejectReason.REGRESSED, r3.rejectReason());

        // 重复提交到 1：幂等 no-op，接受但不前移
        var r4 = broker.commitOffset(group, topic, 1);
        TestSupport.logOffset(group, topic, r4);
        assertTrue(r4.accepted());
        assertEquals(1L, broker.committedOffset(group, topic));

        // 越界提交到 99：UNKNOWN_MESSAGE
        var r5 = broker.commitOffset(group, topic, 99);
        TestSupport.logOffset(group, topic, r5);
        assertFalse(r5.accepted());
        assertEquals(RejectReason.UNKNOWN_MESSAGE, r5.rejectReason());

        // 确认剩余两条后，位点可连续推进到 3
        broker.consumeOnce(group, topic, "c", 10).forEach(a ->
                System.out.println("[DECIDE] rest => " + a.outcome()));
        var r6 = broker.commitOffset(group, topic, 3);
        TestSupport.logOffset(group, topic, r6);
        assertTrue(r6.accepted());
        assertEquals(3L, broker.committedOffset(group, topic));
        broker.close();
    }

    @Test
    void concurrentConsumersNeverCorruptStateAndEffectIsOnce() throws Exception {
        TestSupport.title("concurrent consumers stress");
        IdempotentBroker broker = newBroker();
        String group = "parallel";
        String topic = "work";

        int n = 50;
        for (int i = 0; i < n; i++) {
            broker.enqueue(topic, "m" + i, String.valueOf(i));
        }

        AtomicInteger applied = new AtomicInteger();
        broker.subscribe(group, (id, p) -> {
            applied.incrementAndGet();
            return ConsumeOutcome.success();
        });

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger deliveriesSeen = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final String consumerId = "worker-" + t;
            pool.submit(() -> {
                try {
                    start.await();
                    // 每轮少量拉取并消费；重复多轮以制造竞争
                    for (int round = 0; round < 10; round++) {
                        var got = broker.consumeOnce(group, topic, consumerId, 5);
                        deliveriesSeen.addAndGet(got.size());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS), "workers must finish");
        pool.shutdownNow();

        // 全部 50 条最终 SUCCEEDED，业务生效恰好 50 次（每条一次），总投递结算数 = 50
        var records = broker.records(group, topic);
        long succeeded = records.stream().filter(r -> r.status().name().equals("SUCCEEDED")).count();
        int effects = records.stream().mapToInt(r -> r.businessEffectCount()).sum();
        System.out.println("[CONCURRENCY] messages=" + n + ", succeeded=" + succeeded
                + ", totalBusinessEffects=" + effects + ", ackResults=" + deliveriesSeen.get());
        assertEquals(n, succeeded);
        assertEquals(n, effects, "each message takes effect exactly once");
        assertEquals(n, deliveriesSeen.get(), "every successful delivery accounted exactly once");

        // 全部确认后位点才能整体推进到 n
        var commit = broker.commitOffset(group, topic, n);
        TestSupport.logOffset(group, topic, commit);
        assertTrue(commit.accepted());
        assertEquals(n, broker.committedOffset(group, topic));
        broker.close();
    }
}
