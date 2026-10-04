package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerWorker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ReplayResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景八：运维动作（进度重放 / 死信重投）与同组并发消费共存。
 * 消费者持续运行期间执行回退与重投：业务生效仍恰一次、位点只沿连续终态推进、
 * 操作不与进行中的确认互相覆盖，最终状态一致。
 */
class ConcurrentOpsConsistencyTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy tinyBackoff = failedAttemptNo -> 5L;

    private LocalMessageBroker newBroker(String name, int maxAttempts) {
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve(name + ".json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofSeconds(30), tinyBackoff));
        return broker;
    }

    private static void await(BooleanSupplierWithThrow cond, long timeoutMillis, String what)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("等待超时: " + what);
            }
            Thread.sleep(10);
        }
    }

    @FunctionalInterface
    private interface BooleanSupplierWithThrow {
        boolean getAsBoolean() throws Exception;
    }

    @Test
    void replayWhileConsumersRunningKeepsEffectOnceAndOffsetConsistent() throws Exception {
        LocalMessageBroker broker = newBroker("ops-replay", 3);
        int n = 30;
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }

        Set<String> applied = ConcurrentHashMap.newKeySet();
        AtomicInteger handlerCalls = new AtomicInteger();
        List<ConsumerWorker> pool = new ArrayList<>();
        for (int w = 0; w < 3; w++) {
            pool.add(new ConsumerWorker(broker, "g", "c-" + w, msg -> {
                handlerCalls.incrementAndGet();
                applied.add(msg.id()); // 幂等落效果：重复调用不产生第二次生效
                Thread.sleep((long) (Math.random() * 3));
                return ConsumeOutcome.success();
            }, 2).start());
        }

        // 等消费进行到中段，然后在消费者运行期间执行回退
        await(() -> broker.getCommittedOffset("g") >= 8, 15_000, "至少处理到 offset=8");
        ReplayResult result = broker.replayFrom("g", 2);
        System.out.println("【输入】消费进行中 replayFrom(g, 2)，此前 committed="
                + result.previousCommitted() + "，重置 " + result.resetToPending()
                + " 条，IN_FLIGHT 保留 " + result.leftInFlight() + " 条");
        assertTrue(result.resetToPending() > 0);

        // 重放的消息会被并发 worker 再次投递并确认，位点最终回到末端
        await(() -> broker.getCommittedOffset("g") == n - 1, 15_000, "位点回到末端");
        for (ConsumerWorker w : pool) {
            w.stop(1000);
        }

        System.out.println("【位点】最终 committed=" + broker.getCommittedOffset("g"));
        System.out.println("【判定】处理器调用=" + handlerCalls.get()
                + "（> " + n + "，含重放投递），幂等生效消息数=" + applied.size());
        assertEquals(n - 1L, broker.getCommittedOffset("g"));
        assertEquals(n, applied.size(), "每条消息业务生效恰好一次");
        assertTrue(handlerCalls.get() > n, "重放应产生额外投递");
        for (int i = 0; i < n; i++) {
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i),
                    "offset=" + i + " 必须到达成功终态，不得有半截状态");
        }
        assertTrue(broker.getDeadLetters().isEmpty());
    }

    @Test
    void redeliverWhileConsumersRunningCompletesPoisonMessageOnce() throws Exception {
        LocalMessageBroker broker = newBroker("ops-redeliver", 2);
        int n = 20;
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }

        Set<String> applied = ConcurrentHashMap.newKeySet();
        AtomicBoolean poisonCured = new AtomicBoolean(false);
        AtomicInteger poisonEffects = new AtomicInteger();
        List<ConsumerWorker> pool = new ArrayList<>();
        for (int w = 0; w < 3; w++) {
            pool.add(new ConsumerWorker(broker, "g", "c-" + w, msg -> {
                Thread.sleep((long) (Math.random() * 3));
                if (msg.id().equals("m-7") && !poisonCured.get()) {
                    return ConsumeOutcome.retry("POISON_NOT_CURED_YET");
                }
                if (msg.id().equals("m-7")) {
                    poisonEffects.incrementAndGet();
                }
                applied.add(msg.id());
                return ConsumeOutcome.success();
            }, 2).start());
        }

        // m-7 两次失败后进死信；其余消息全部成功，位点越过死信到达末端
        await(() -> broker.getCommittedOffset("g") == n - 1, 15_000, "位点越过死信到达末端");
        await(() -> broker.getMessageState("g", 7) == MessageState.DEAD, 15_000, "m-7 进入死信");
        assertEquals(1, broker.getDeadLetters().size());
        System.out.println("【输入】m-7 已死信（其余全部成功），消费者仍在运行，执行人工重投");

        // 消费者运行期间重投死信，并“修复”业务
        poisonCured.set(true);
        long offset = broker.redeliverDeadLetter("g", "m-7");
        assertEquals(7L, offset);
        await(() -> broker.getMessageState("g", 7) == MessageState.SUCCEEDED, 15_000,
                "重投的 m-7 被并发 worker 处理成功");
        for (ConsumerWorker w : pool) {
            w.stop(1000);
        }

        System.out.println("【位点】最终 committed=" + broker.getCommittedOffset("g"));
        System.out.println("【判定】m-7 业务生效次数=" + poisonEffects.get()
                + " attemptCount=" + broker.getAttemptCount("g", 7)
                + " 死信记录数=" + broker.getDeadLetters().size());

        assertEquals(1, poisonEffects.get(), "重投后业务生效恰好一次");
        assertEquals(3, broker.getAttemptCount("g", 7), "2 次自动 + 1 次重投");
        assertEquals(1, broker.getDeadLetters().size(), "原死信记录保留");
        assertEquals(n, applied.size());
        assertEquals(n - 1L, broker.getCommittedOffset("g"));
        for (int i = 0; i < n; i++) {
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i));
        }
    }
}
