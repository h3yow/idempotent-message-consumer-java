package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerWorker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryKind;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.ops.ReplayRejectedException;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运维场景三：死信重投 / 进度回退与同组并发消费同时进行时的安全性。
 * 覆盖：
 * 1) 消费进行中（始终存在未确认租约）回退被 INFLIGHT 拒绝，水线与消息状态保持原样；
 * 2) 多个线程同时死信重投同一条消息，只有一个能改变状态，不会出现并发重复发放/半完成进度；
 * 3) 消费全部完成后回退 + 幂等重放，业务效果仍然每条恰好一次，位点恰好到末端。
 */
class ReplayConcurrencyTest {

    @TempDir
    Path tmp;

    private LocalMessageBroker broker(String name, int maxAttempts) {
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve(name + ".json")));
        BackoffPolicy fixedBackoff = failed -> 5L;
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofSeconds(30), fixedBackoff));
        return broker;
    }

    @Test
    void rewindWhileConsumersHaveInflightLeasesIsRejectedAndLeavesEverythingIntact()
            throws Exception {
        LocalMessageBroker broker = broker("conc-rewind", 3);
        int n = 30;
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }

        // 前 8 条立即处理完，把水线推到 7；从第 9 条起挂住，保证既存在已推进的水线、
        // 又始终存在 IN_FLIGHT 未确认租约
        CountDownLatch hold = new CountDownLatch(1);
        CountDownLatch barrier = new CountDownLatch(8);
        com.github.h3yow.idempotentmessageconsumerjava.mq.handler.MessageHandler handler = msg -> {
            long offset = Long.parseLong(msg.id().substring(2));
            if (offset < 8) {
                barrier.countDown();
                return ConsumeOutcome.success();
            }
            barrier.countDown();
            hold.await();
            return ConsumeOutcome.success();
        };
        ConsumerWorker w1 = new ConsumerWorker(broker, "g", "cw-1", handler, 5).start();
        ConsumerWorker w2 = new ConsumerWorker(broker, "g", "cw-2", handler, 5).start();
        assertTrue(barrier.await(5, TimeUnit.SECONDS));
        long deadline0 = System.currentTimeMillis() + 5_000;
        while (broker.getCommittedOffset("g") < 7 && System.currentTimeMillis() < deadline0) {
            Thread.sleep(20);
        }
        assertEquals(7L, broker.getCommittedOffset("g"), "前置 8 条消息先把水线推到 7");

        // 消费进行中（offset>=8 存在未确认租约），回退到任何更早位置都必须被 INFLIGHT 拒绝
        long committedBefore = broker.getCommittedOffset("g");
        ReplayRejectedException rejected = null;
        for (long target = committedBefore; target >= 0; target--) {
            try {
                broker.rewindCommittedOffset("g", target, "ops", "rewind during traffic");
                fail("存在未确认租约时回退必须被拒绝: target=" + target);
            } catch (ReplayRejectedException ex) {
                assertEquals(DecisionCode.REWIND_REJECTED_INFLIGHT, ex.code(),
                        "target=" + target);
                rejected = ex;
            }
        }
        // 越界原因在并发期也必须仍可区分（range 检查先于 in-flight 检查）
        ReplayRejectedException range = assertThrows(ReplayRejectedException.class,
                () -> broker.rewindCommittedOffset("g", 10_000L, "ops", "x"));

        System.out.println("【输入】" + n + " 条消息、2 worker 并发，处理期间存在未确认租约");
        System.out.println("【判定】并发期回退 -> " + (rejected == null ? null : rejected.code())
                + " | " + (rejected == null ? null : rejected.getMessage()));
        System.out.println("【判定】并发期越界回退 -> " + range.code());
        System.out.println("【位点】拒绝后 committed=" + broker.getCommittedOffset("g")
                + "（保持 " + committedBefore + "）");

        assertEquals(DecisionCode.REWIND_REJECTED_RANGE, range.code());
        assertEquals(committedBefore, broker.getCommittedOffset("g"),
                "并发期被拒绝的回退不得动水线");
        assertTrue(broker.getAllReplays("g").isEmpty(), "被拒绝的操作不留重放记录");

        // 放开处理，先等水线到末端（不中断 worker），再优雅停止
        hold.countDown();
        long finishDeadline = System.currentTimeMillis() + 15_000;
        while (broker.getCommittedOffset("g") < n - 1
                && System.currentTimeMillis() < finishDeadline) {
            Thread.sleep(20);
        }
        w1.stop(5_000);
        w2.stop(5_000);
        assertEquals(n - 1L, broker.getCommittedOffset("g"));
        for (int i = 0; i < n; i++) {
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i));
        }
        assertTrue(broker.getDeadLetters().isEmpty());
    }

    @Test
    void concurrentDeadLetterReplaysOnlyOneWinsAndNoDuplicateDispatch() throws Exception {
        // 零退避，确保 while-poll 能把 3 次尝试真正耗尽到 DEAD
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve("conc-replay.json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", 3,
                Duration.ofSeconds(30), failed -> 0L));
        broker.enqueue(new Message("m-dead", "orders", "p", 1));
        LeasedMessage lease;
        while ((lease = broker.poll("g", "c")) != null) {
            broker.acknowledgeFailure(lease, true, "X");
        }
        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final String op = "ops-" + i;
            pool.submit(() -> {
                try {
                    start.await();
                    broker.replayDeadLetter("g", 0, op, "concurrent compensation");
                    accepted.incrementAndGet();
                } catch (ReplayRejectedException ex) {
                    assertEquals(DecisionCode.REPLAY_REJECTED_NOT_DEAD, ex.code());
                    rejected.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdownNow();

        System.out.println("【输入】" + threads + " 个线程同时死信重投 offset=0");
        System.out.println("【判定】接受=" + accepted.get() + " 拒绝=" + rejected.get());
        System.out.println("【依据】重放记录数=" + broker.getReplays("g", 0).size());

        assertEquals(1, accepted.get(), "同一时刻只允许一个重放生效");
        assertEquals(threads - 1, rejected.get(), "其余并发请求发现消息已不是 DEAD，被拒绝");
        assertEquals(1, broker.getReplays("g", 0).size());
        // 只有一次重放投递
        LeasedMessage only = broker.poll("g", "c");
        assertNotNull(only);
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, only.deliveryKind());
        assertEquals(4, only.attemptNo());
        broker.acknowledgeSuccess(only);
        assertNull(broker.poll("g", "c"), "不存在并发重复发放的第二份重放投递");
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
    }

    @Test
    void replayDuringConcurrentConsumersStillTakesEffectOnce() throws Exception {
        LocalMessageBroker broker = broker("conc-effect", 3);
        int n = 40;
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        ConcurrentHashMap<String, AtomicInteger> effect = new ConcurrentHashMap<>();

        com.github.h3yow.idempotentmessageconsumerjava.mq.handler.MessageHandler handler = msg -> {
            long offset = Long.parseLong(msg.id().substring(2));
            if (broker.hasBusinessEffectTaken("g", offset)) {
                return ConsumeOutcome.success(); // 重放抑制
            }
            effect.computeIfAbsent(msg.id(), k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep((long) (Math.random() * 3));
            return ConsumeOutcome.success();
        };
        List<ConsumerWorker> workers = new java.util.ArrayList<>();
        for (int w = 0; w < 4; w++) {
            workers.add(new ConsumerWorker(broker, "g", "cw-" + w, handler, 2).start());
        }
        long deadline = System.currentTimeMillis() + 15_000;
        while (broker.getCommittedOffset("g") < n - 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(n - 1L, broker.getCommittedOffset("g"));

        // 消费仍在运行时回退到一半位置重放
        broker.rewindCommittedOffset("g", n / 2, "ops", "concurrent recompute");
        deadline = System.currentTimeMillis() + 15_000;
        while (broker.getCommittedOffset("g") < n - 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        for (ConsumerWorker w : workers) {
            w.stop(2_000);
        }

        System.out.println("【输入】" + n + " 条消息 4 worker 并发消费，消费运行中回退到 " + (n / 2));
        System.out.println("【位点】最终 committed=" + broker.getCommittedOffset("g"));
        int totalEffect = effect.values().stream().mapToInt(AtomicInteger::get).sum();
        System.out.println("【判定】业务生效总次数=" + totalEffect + "（期望 " + n + "）");
        long suppressed = broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.DUPLICATE_EFFECT_SUPPRESSED).count();
        System.out.println("【依据】幂等抑制轨迹条数=" + suppressed + "（期望 " + (n - n / 2) + "）");

        assertEquals(n - 1L, broker.getCommittedOffset("g"));
        assertEquals(n, totalEffect, "并发 + 重放下，业务效果仍每条消息只算一次");
        for (int i = 0; i < n; i++) {
            assertEquals(1, effect.get("m-" + i).get(), "m-" + i + " 重复生效");
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i));
        }
    }
}
