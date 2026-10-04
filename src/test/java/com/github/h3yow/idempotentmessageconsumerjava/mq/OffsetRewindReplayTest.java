package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryKind;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ReplayRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.ops.ReplayRejectedException;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运维场景二：消费进度回退重放。
 * 固定时钟 + 零退避，结论确定可复现。覆盖：
 * 回退后以前成功过的消息被标记为重放重新投递（不是新消息）、业务效果全组只算一次；
 * 回退只影响目标组；越界/不是更早位置/区间内有未确认消息三类拒绝原因分别正确且不留半完成进度；
 * 回退重放后失败仍受 maxAttempts 约束、再次死信；回退后水线可随重新确认再次前进。
 */
class OffsetRewindReplayTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker broker(String file, int maxAttempts) {
        Clock clock = new TickClock(4_000_000L, 100L);
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve(file)), clock);
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofSeconds(10), noBackoff));
        return broker;
    }

    private static final class TickClock extends Clock {
        private long now;
        private final long step;
        TickClock(long start, long step) { this.now = start; this.step = step; }
        @Override public long millis() { now += step; return now; }
        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis()); }
    }

    /** 幂等处理器：用 broker 的生效护栏抑制重复业务写，同时统计“写操作次数”。 */
    private ConsumeOutcome idempotentHandler(LocalMessageBroker broker, LeasedMessage lease,
                                             AtomicInteger businessWrites) {
        if (broker.hasBusinessEffectTaken("g", lease.offset())) {
            return ConsumeOutcome.success(); // 重放：已生效，跳过业务写
        }
        businessWrites.incrementAndGet();
        return ConsumeOutcome.success();
    }

    @Test
    void rewindRedeliversSucceededMessagesAsReplayButBusinessEffectCountsOnce() {
        LocalMessageBroker broker = broker("rewind-ok.json", 3);
        int n = 5;
        AtomicInteger writes = new AtomicInteger();
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        LeasedMessage lease;
        while ((lease = broker.poll("g", "c1")) != null) {
            broker.complete(lease, idempotentHandler(broker, lease, writes), null);
        }
        assertEquals(n - 1L, broker.getCommittedOffset("g"));
        assertEquals(n, writes.get());

        // 回退到 offset=2：消息 2、3、4 重新投递（重放），0、1 不动
        List<ReplayRecord> reopened = broker.rewindCommittedOffset("g", 2, "ops-carol", "recompute");

        System.out.println("【输入】5 条消息全部成功后，回退进度到 offset=2（operator=ops-carol）");
        System.out.println("【位点】回退后 committed=" + broker.getCommittedOffset("g") + "（期望 1）");
        System.out.println("【判定】重新开放的终态消息数=" + reopened.size() + "（期望 3）");
        assertEquals(3, reopened.size());
        assertEquals(1L, broker.getCommittedOffset("g"));
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 2));
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0), "回退点之前的消息不动");
        assertEquals(DeliveryKind.OFFSET_REWIND, reopened.get(0).kind());
        assertEquals(1, reopened.get(0).replayNo());

        int replayDeliveries = 0;
        while ((lease = broker.poll("g", "c1")) != null) {
            assertTrue(lease.offset() >= 2);
            assertEquals(DeliveryKind.OFFSET_REWIND, lease.deliveryKind(),
                    "回退后的投递必须看得出是重放，而不是新消息");
            assertEquals(2, lease.attemptNo(),
                    "重放是同一消息的第二次处理：首次 attemptNo=1，重放 attemptNo=2");
            replayDeliveries++;
            broker.complete(lease, idempotentHandler(broker, lease, writes), null);
        }

        System.out.println("【判定】重放投递数=" + replayDeliveries + " 业务写操作总次数="
                + writes.get() + "（期望仍为 5）");
        System.out.println("【位点】重放确认完 committed=" + broker.getCommittedOffset("g")
                + "（期望 4）");
        assertEquals(3, replayDeliveries);
        assertEquals(n, writes.get(), "业务效果每组每条消息最多一次，重放不得产生第二次生效");
        assertEquals(n - 1L, broker.getCommittedOffset("g"));
        for (int i = 0; i < n; i++) {
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i));
        }
        // 每条重放消息都留下了“效果已被幂等抑制”的判定轨迹
        for (int i = 2; i < n; i++) {
            TraceEvent suppressed = broker.lastTraceFor("g", "m-" + i);
            assertEquals(DecisionCode.DUPLICATE_EFFECT_SUPPRESSED, suppressed.code(),
                    "offset=" + i + " 的第二次成功必须记为幂等抑制");
            AttemptRecord last = broker.getAttempts("g", i)
                    .get(broker.getAttempts("g", i).size() - 1);
            assertEquals(DeliveryKind.OFFSET_REWIND, last.deliveryKind());
        }
        // 没产生新的死信
        assertTrue(broker.getDeadLetters().isEmpty());
        TraceEvent accepted = broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.OFFSET_REWIND_ACCEPTED)
                .findFirst().orElseThrow();
        assertTrue(accepted.detail().contains("ops-carol"));
    }

    @Test
    void invalidRewindsAreRejectedWithDistinctReasonsAndNoHalfDoneState() {
        // 用真实时钟 + 较长租约，避免固定步进时钟在密集 poll 间误触发租约超时回收
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve("rewind-reject.json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", 3,
                Duration.ofHours(1), noBackoff));
        for (int i = 0; i < 5; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        // 先成功 0、1，水线到 1；租出 2 不确认
        LeasedMessage l0 = broker.poll("g", "c1");
        broker.acknowledgeSuccess(l0);
        LeasedMessage l1 = broker.poll("g", "c1");
        broker.acknowledgeSuccess(l1);
        LeasedMessage l2 = broker.poll("g", "c1");
        assertNotNull(l2);
        assertEquals(2L, l2.offset());

        System.out.println("【输入】committed=1，offset=2 IN_FLIGHT 未确认");

        ReplayRejectedException range = assertThrows(ReplayRejectedException.class,
                () -> broker.rewindCommittedOffset("g", -1, "ops", "x"));
        ReplayRejectedException range2 = assertThrows(ReplayRejectedException.class,
                () -> broker.rewindCommittedOffset("g", 100, "ops", "x"));
        ReplayRejectedException notBehind = assertThrows(ReplayRejectedException.class,
                () -> broker.rewindCommittedOffset("g", 2, "ops", "x"));
        ReplayRejectedException inflight = assertThrows(ReplayRejectedException.class,
                () -> broker.rewindCommittedOffset("g", 0, "ops", "x"));

        System.out.println("【判定】target=-1   -> " + range.code());
        System.out.println("【判定】target=100  -> " + range2.code());
        System.out.println("【判定】target=2(不早于水线+1=2) -> " + notBehind.code());
        System.out.println("【判定】target=0(区间含 IN_FLIGHT=2) -> " + inflight.code()
                + " | " + inflight.getMessage());

        assertEquals(DecisionCode.REWIND_REJECTED_RANGE, range.code());
        assertEquals(DecisionCode.REWIND_REJECTED_RANGE, range2.code());
        assertEquals(DecisionCode.REWIND_REJECTED_NOT_BEHIND, notBehind.code());
        assertEquals(DecisionCode.REWIND_REJECTED_INFLIGHT, inflight.code());
        assertEquals(1L, broker.getCommittedOffset("g"), "拒绝后水线保持原样");
        assertEquals(MessageState.IN_FLIGHT, broker.getMessageState("g", 2),
                "未确认租约不被拒绝操作触碰");
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
        assertTrue(broker.getAllReplays("g").isEmpty(), "拒绝不得留下任何重放记录");

        // 未确认消息确认完后，回退到 0 才能成功（原子性：一旦接受就完整生效）
        broker.acknowledgeSuccess(l2);
        for (int i = 3; i <= 4; i++) {
            LeasedMessage l = broker.poll("g", "c1");
            assertNotNull(l);
            broker.acknowledgeSuccess(l);
        }
        assertEquals(4L, broker.getCommittedOffset("g"));
        List<ReplayRecord> done = broker.rewindCommittedOffset("g", 0, "ops", "full replay");
        assertEquals(5, done.size());
        assertEquals(-1L, broker.getCommittedOffset("g"));
        for (int i = 0; i < 5; i++) {
            assertEquals(MessageState.PENDING, broker.getMessageState("g", i));
        }
    }

    @Test
    void rewindAffectsOnlyTargetGroupAndOtherGroupUntouched() {
        Clock clock = new TickClock(5_000_000L, 100L);
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve("rewind-groups.json")), clock);
        broker.registerGroup(new ConsumerGroupConfig("g1", "orders", 3,
                Duration.ofSeconds(10), noBackoff));
        broker.registerGroup(new ConsumerGroupConfig("g2", "orders", 3,
                Duration.ofSeconds(10), noBackoff));
        for (int i = 0; i < 3; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        for (String g : new String[]{"g1", "g2"}) {
            LeasedMessage l;
            while ((l = broker.poll(g, "c")) != null) {
                broker.acknowledgeSuccess(l);
            }
            assertEquals(2L, broker.getCommittedOffset(g));
        }
        broker.rewindCommittedOffset("g1", 0, "ops", "only g1");

        System.out.println("【输入】g1/g2 水线都=2，只回退 g1 到 0");
        System.out.println("【位点】g1 committed=" + broker.getCommittedOffset("g1")
                + " g2 committed=" + broker.getCommittedOffset("g2"));

        assertEquals(-1L, broker.getCommittedOffset("g1"));
        assertEquals(2L, broker.getCommittedOffset("g2"), "其他组进度不受影响");
        assertNotNull(broker.poll("g1", "c"));
        assertNull(broker.poll("g2", "c"), "g2 没有消息需要重放");
        assertEquals(0, broker.getAllReplays("g2").size());
        assertEquals(3, broker.getAllReplays("g1").size());
    }

    @Test
    void replayAfterRewindStillRespectsMaxAttemptsAndReDies() {
        LocalMessageBroker broker = broker("rewind-dead.json", 2);
        broker.enqueue(new Message("m-one", "orders", "p", 1));
        LeasedMessage first = broker.poll("g", "c1");
        broker.acknowledgeSuccess(first);
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));

        // 回退重放，这次处理器要求直接不可重试死信
        broker.rewindCommittedOffset("g", 0, "ops", "replay with bad data");
        LeasedMessage replay = broker.poll("g", "c1");
        assertNotNull(replay);
        assertEquals(DeliveryKind.OFFSET_REWIND, replay.deliveryKind());
        broker.acknowledgeFailure(replay, false, "INVALID_ON_RECOMPUTE");

        System.out.println("【输入】maxAttempts=2，成功后回退重放，重放时判定不可重试");
        System.out.println("【位点】committed=" + broker.getCommittedOffset("g")
                + " state=" + broker.getMessageState("g", 0));
        System.out.println("【依据】succeededOnce 标记保留="
                + broker.hasBusinessEffectTaken("g", 0) + "（重放死信也不能否定已生效事实）");

        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));
        assertEquals(0L, broker.getCommittedOffset("g"), "死信是终态，水线仍可越过");
        assertEquals(1, broker.getDeadLetters().size());
        assertEquals(1, broker.getDeadLetters().get(0).replayNo(), "是重放后产生的死信");
        assertEquals("NON_RETRYABLE_BY_HANDLER", broker.getDeadLetters().get(0).cause());
        assertTrue(broker.hasBusinessEffectTaken("g", 0),
                "重放后死信不能抹掉“业务曾生效一次”的事实");
        // 该消息既有成功尝试，又有重放后的不可重试尝试，审计链完整
        List<AttemptRecord> attempts = broker.getAttempts("g", 0);
        assertEquals(AttemptResult.SUCCESS, attempts.get(0).result());
        assertEquals(AttemptResult.NON_RETRYABLE_FAILURE, attempts.get(1).result());
        // 死信后仍可人工死信重投再补偿一次
        broker.replayDeadLetter("g", 0, "ops", "compensate after replay-death");
        LeasedMessage again = broker.poll("g", "c1");
        assertNotNull(again);
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, again.deliveryKind());
        broker.acknowledgeSuccess(again);
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
    }
}
