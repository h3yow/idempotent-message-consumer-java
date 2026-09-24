package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerWorker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.OffsetCommitRejectedException;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景三：并发消费下的位点管理。
 * 多 worker 并发处理时，业务只能生效一次，位点只能沿“连续终态”前进；
 * 回退提交、跨过未确认消息的提交、越界提交必须被明确拒绝（原因可区分）。
 */
class ConcurrentConsumeOffsetTest {

    @TempDir
    Path tmp;

    private LocalMessageBroker newBroker(String name) {
        BackoffPolicy fixedBackoff = failed -> 5L;
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve(name + ".json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", 3,
                Duration.ofSeconds(30), fixedBackoff));
        return broker;
    }

    @Test
    void concurrentConsumersEachMessageTakesEffectOnceAndOffsetReachesEnd() throws Exception {
        LocalMessageBroker broker = newBroker("conc");
        int n = 60;
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "payload-" + i, i));
        }

        ConcurrentHashMap<String, AtomicInteger> effectPerMessage = new ConcurrentHashMap<>();
        AtomicInteger handled = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(n);

        com.github.h3yow.idempotentmessageconsumerjava.mq.handler.MessageHandler handler = msg -> {
            effectPerMessage.computeIfAbsent(msg.id(), k -> new AtomicInteger()).incrementAndGet();
            handled.incrementAndGet();
            done.countDown();
            // 制造随机处理时长，放大乱序确认概率
            Thread.sleep((long) (Math.random() * 5));
            return com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome.success();
        };

        int workers = 4;
        java.util.List<ConsumerWorker> pool = new java.util.ArrayList<>();
        for (int w = 0; w < workers; w++) {
            pool.add(new ConsumerWorker(broker, "g", "consumer-" + w, handler, 2).start());
        }

        assertTrue(done.await(20, java.util.concurrent.TimeUnit.SECONDS),
                "所有消息应在超时前处理完成");
        // 等待位点自动推进到末端
        long deadline = System.currentTimeMillis() + 5_000;
        while (broker.getCommittedOffset("g") < n - 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        for (ConsumerWorker w : pool) {
            w.stop(1000);
        }

        System.out.println("【输入】消息数=" + n + " 并发worker=" + workers);
        System.out.println("【位点】最终 committed=" + broker.getCommittedOffset("g")
                + " 期望=" + (n - 1));
        System.out.println("【判定】处理器调用总次数=" + handled.get());

        Map<String, Integer> repeated = new HashMap<>();
        effectPerMessage.forEach((id, c) -> {
            if (c.get() != 1) {
                repeated.put(id, c.get());
            }
        });
        System.out.println("【依据】生效次数 != 1 的消息=" + repeated);

        assertEquals(n, handled.get(), "每条消息恰好被业务处理一次");
        assertTrue(repeated.isEmpty(), "存在重复生效: " + repeated);
        assertEquals(n - 1L, broker.getCommittedOffset("g"),
                "并发乱序确认后位点必须恰好到达最后一条，不能越过任何未确认消息");
        for (int i = 0; i < n; i++) {
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i));
        }
        assertTrue(broker.getDeadLetters().isEmpty());
    }

    @Test
    void rollbackAndGapAndOutOfRangeCommitsAreRejectedWithDistinctReasons() throws Exception {
        LocalMessageBroker broker = newBroker("reject");
        for (int i = 0; i < 5; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }

        // 先正常处理 offset=0，位点推进到 0
        var lease0 = broker.poll("g", "c1");
        broker.acknowledgeSuccess(lease0);
        assertEquals(0L, broker.getCommittedOffset("g"));

        // 租出 offset=1 但不确认 —— 制造“未确认缺口”
        var lease1 = broker.poll("g", "c1");
        assertNotNull(lease1);
        assertEquals(1L, lease1.offset());

        // 1) 回退提交：0 < 当前 0 不构成回退，用 -1 与已提交后的回退验证
        OffsetCommitRejectedException rollback = assertThrows(OffsetCommitRejectedException.class,
                () -> broker.commitOffset("g", -1));
        // 2) 跨越未确认消息：offset=1 仍 IN_FLIGHT，提交 2 必须被拒
        OffsetCommitRejectedException gap = assertThrows(OffsetCommitRejectedException.class,
                () -> broker.commitOffset("g", 2));
        // 3) 越界提交
        OffsetCommitRejectedException range = assertThrows(OffsetCommitRejectedException.class,
                () -> broker.commitOffset("g", 100));

        System.out.println("【输入】committed=0, offset=1 处于 IN_FLIGHT 未确认");
        System.out.println("【判定】回退(-1) -> " + rollback.code());
        System.out.println("【判定】跨缺口(2) -> " + gap.code() + " | " + gap.getMessage());
        System.out.println("【判定】越界(100) -> " + range.code());
        System.out.println("【位点】被拒绝后 committed 仍为 " + broker.getCommittedOffset("g"));

        assertEquals(DecisionCode.OFFSET_COMMIT_REJECTED_ROLLBACK, rollback.code());
        assertEquals(DecisionCode.OFFSET_COMMIT_REJECTED_GAP, gap.code());
        assertEquals(DecisionCode.OFFSET_COMMIT_REJECTED_RANGE, range.code());
        assertEquals(0L, broker.getCommittedOffset("g"), "拒绝提交不得改变位点");
        assertEquals(MessageState.IN_FLIGHT, broker.getMessageState("g", 1));

        // 缺口补齐后（offset=1、2 均确认），提交到 2 成功
        broker.acknowledgeSuccess(lease1);
        var lease2 = broker.poll("g", "c1");
        assertNotNull(lease2);
        assertEquals(2L, lease2.offset());
        broker.acknowledgeSuccess(lease2);
        broker.commitOffset("g", 2);
        System.out.println("【判定】offset=1、2 确认后显式提交 2，committed="
                + broker.getCommittedOffset("g"));
        assertEquals(2L, broker.getCommittedOffset("g"));

        // 此时再提交 2 不是回退（幂等无操作），提交 1 才是回退
        OffsetCommitRejectedException backAgain = assertThrows(
                OffsetCommitRejectedException.class, () -> broker.commitOffset("g", 1));
        assertEquals(DecisionCode.OFFSET_COMMIT_REJECTED_ROLLBACK, backAgain.code());
    }
}
