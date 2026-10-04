package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运维场景一：死信重投（人工补偿）。
 * 固定时钟 + 零退避，结论确定可复现。覆盖：
 * 重放后成功（业务最多生效一次、位点重新越过）；
 * 重放后再失败仍受 maxAttempts 约束（不会因人工重投获得无限机会）；
 * 非死信/越界重投以不同编码拒绝且状态不变；
 * 原始死信记录不被抹掉、审计轨迹能区分“人工重放”以及是第几次处理。
 */
class DeadLetterReplayTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker broker(int maxAttempts) {
        Clock clock = new TickClock(2_000_000L, 100L);
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve("dead-replay.json")), clock);
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofSeconds(10), noBackoff));
        return broker;
    }

    private void exhaustToDead(LocalMessageBroker broker, String messageId) {
        broker.enqueue(new Message(messageId, "orders", "flaky-payload", 1));
        LeasedMessage lease;
        while ((lease = broker.poll("g", "c1")) != null) {
            broker.acknowledgeFailure(lease, true, "REMOTE_500");
        }
        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));
    }

    /** 每次 millis() 调用推进固定步长的时钟，时间断言确定。 */
    private static final class TickClock extends Clock {
        private long now;
        private final long step;

        TickClock(long start, long step) {
            this.now = start;
            this.step = step;
        }

        @Override
        public long millis() {
            now += step;
            return now;
        }

        @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis()); }
    }

    @Test
    void replayDeadLetterThenSuccessKeepsSingleEffectAndKeepsOriginalRecord() {
        LocalMessageBroker broker = broker(3);
        exhaustToDead(broker, "m-dead-ok");
        assertEquals(0L, broker.getCommittedOffset("g"), "死信也是终态，水线已越过 offset=0");
        assertEquals(1, broker.getDeadLetters().size());
        assertEquals(0, broker.getDeadLetters().get(0).replayNo());

        // 人工重投
        ReplayRecord replay = broker.replayDeadLetter("g", 0, "ops-alice", "manual compensation");

        System.out.println("【输入】offset=0 已死信（3 次尝试耗尽），人工重投 operator=ops-alice");
        System.out.println("【判定】replay 记录: no=" + replay.replayNo() + " kind=" + replay.kind()
                + " operator=" + replay.operator() + " reason=" + replay.reason());
        System.out.println("【位点】重投后 committed=" + broker.getCommittedOffset("g")
                + " state=" + broker.getMessageState("g", 0));

        assertEquals(1, replay.replayNo());
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, replay.kind());
        assertEquals(-1L, broker.getCommittedOffset("g"),
                "重投位点在原水线之前时，水线必须同步退回到该位点之前，才能重新投递");
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 0));
        // 原始死信记录必须还在，没有被抹掉
        assertEquals(1, broker.getDeadLetters().size(), "原始死信记录不得被重投删除");

        LeasedMessage redelivered = broker.poll("g", "c2");
        assertNotNull(redelivered);
        System.out.println("【判定】重放投递: offset=" + redelivered.offset()
                + " attemptNo=" + redelivered.attemptNo() + " kind=" + redelivered.deliveryKind()
                + " replayNo=" + redelivered.replayNo() + " messageId=" + redelivered.message().id());
        assertEquals(0L, redelivered.offset(), "必须是同一条消息、同一个位点的延续，不是新消息");
        assertEquals("m-dead-ok", redelivered.message().id());
        assertEquals(4, redelivered.attemptNo(), "attemptNo 延续历史，第 4 次处理");
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, redelivered.deliveryKind());
        assertEquals(1, redelivered.replayNo(), "审计上必须看得出这是第 1 次人工重放");
        broker.acknowledgeSuccess(redelivered);

        System.out.println("【位点】重放成功后 committed=" + broker.getCommittedOffset("g")
                + " state=" + broker.getMessageState("g", 0));
        System.out.println("【依据】死信表大小=" + broker.getDeadLetters().size()
                + "（原始死信保留）, 尝试记录数=" + broker.getAttempts("g", 0).size());
        for (AttemptRecord a : broker.getAttempts("g", 0)) {
            System.out.println("    attemptNo=" + a.attemptNo() + " result=" + a.result()
                    + " kind=" + a.deliveryKind() + " reason=" + a.reason());
        }

        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
        assertEquals(0L, broker.getCommittedOffset("g"), "重新成功后水线再次越过该位点");
        assertEquals(1, broker.getDeadLetters().size(), "成功也不清理原始死信记录");
        TraceEvent last = broker.lastTraceFor("g", "m-dead-ok");
        assertEquals(DecisionCode.ACK_SUCCEEDED, last.code());
        TraceEvent accepted = broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.DEAD_LETTER_REPLAY_ACCEPTED)
                .findFirst().orElseThrow();
        assertTrue(accepted.detail().contains("ops-alice"));
        TraceEvent dispatch = broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.REPLAY_DISPATCHED).findFirst().orElseThrow();
        assertTrue(dispatch.detail().contains("attemptNo=4"));
    }

    @Test
    void replayDeadLetterThenFailureStillBoundedByMaxAttempts() {
        LocalMessageBroker broker = broker(3);
        exhaustToDead(broker, "m-dead-fail");

        broker.replayDeadLetter("g", 0, "ops-bob", "retry once manually");
        // 语义：人工重投每次只给“一次”受控投递机会，且 attemptNo 延续历史计数。
        // 原始 3 次已耗尽（attemptCount==maxAttempts），所以重放这次再失败时，
        // 自动重试剩余额度 = maxAttempts - attemptCount = 0：立即再次死信，
        // 不会因为有人工重投就白送新的自动重试额度，更不会无限重投。
        int replayPolls = 0;
        LeasedMessage lease;
        while ((lease = broker.poll("g", "c1")) != null) {
            replayPolls++;
            broker.acknowledgeFailure(lease, true, "STILL_500");
        }

        List<AttemptRecord> attempts = broker.getAttempts("g", 0);
        System.out.println("【输入】maxAttempts=3，原始 3 次耗尽后人工重投，之后持续失败");
        System.out.println("【判定】重放后投递次数=" + replayPolls
                + "（重放只给 1 次；失败时自动重试剩余额度=0，立即再死信）");
        System.out.println("【依据】总尝试数=" + attempts.size());
        for (AttemptRecord a : attempts) {
            System.out.println("    attemptNo=" + a.attemptNo() + " result=" + a.result()
                    + " kind=" + a.deliveryKind() + " reason=" + a.reason());
        }
        List<DeadLetter> deadLetters = broker.getDeadLetters();
        System.out.println("【依据】死信记录数=" + deadLetters.size()
                + "（原始 replayNo=0 + 重放后 replayNo=1）");

        assertEquals(4, attempts.size(), "3(原始)+1(人工重放)，重放不赠送自动重试额度");
        assertEquals(1, replayPolls);
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, attempts.get(3).deliveryKind());
        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));
        assertEquals(2, deadLetters.size(), "原始死信 + 重放后再次死信，各一条");
        assertEquals(0, deadLetters.get(0).replayNo());
        assertEquals(1, deadLetters.get(1).replayNo());
        assertEquals("ops-bob", deadLetters.get(1).operator());
        assertEquals("MAX_ATTEMPTS_REACHED", deadLetters.get(1).cause());
        TraceEvent exhaust = broker.lastTraceFor("g", "m-dead-fail");
        assertEquals(DecisionCode.NACK_RETRY_EXHAUSTED, exhaust.code());
        assertTrue(exhaust.detail().contains("replayNo=1"));

        // 再 poll 拿不到；第二次人工重放仍是只给一次、attemptNo 继续延续、轮次可区分
        assertNull(broker.poll("g", "c1"));
        broker.replayDeadLetter("g", 0, "ops-bob", "second compensation");
        LeasedMessage secondRound = broker.poll("g", "c1");
        assertNotNull(secondRound);
        assertEquals(5, secondRound.attemptNo());
        assertEquals(2, secondRound.replayNo(), "第 2 次人工重放必须可区分");
        broker.acknowledgeFailure(secondRound, false, "NOW_PERMANENT");
        assertEquals(3, broker.getDeadLetters().size());
        assertEquals(2, broker.getDeadLetters().get(2).replayNo());
        assertEquals("NON_RETRYABLE_BY_HANDLER", broker.getDeadLetters().get(2).cause());
    }

    @Test
    void replayNonDeadOrMissingOffsetIsRejectedWithDistinctCodesAndStateUntouched() {
        LocalMessageBroker broker = broker(3);
        broker.enqueue(new Message("m-pending", "orders", "p", 1));

        // 1) 目标位点存在但不是死信（PENDING）
        ReplayRejectedException notDead = assertThrows(ReplayRejectedException.class,
                () -> broker.replayDeadLetter("g", 0, "ops", "nope"));
        // 2) 位点越界
        ReplayRejectedException missing = assertThrows(ReplayRejectedException.class,
                () -> broker.replayDeadLetter("g", 99, "ops", "nope"));

        System.out.println("【输入】offset=0 PENDING 时请求死信重投；offset=99 越界");
        System.out.println("【判定】非死信 -> " + notDead.code());
        System.out.println("【判定】越界   -> " + missing.code());

        assertEquals(DecisionCode.REPLAY_REJECTED_NOT_DEAD, notDead.code());
        assertEquals(DecisionCode.REPLAY_REJECTED_OFFSET_MISSING, missing.code());
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 0), "拒绝不得改状态");
        assertEquals(-1L, broker.getCommittedOffset("g"), "拒绝不得动水线");
        assertTrue(broker.getReplays("g", 0).isEmpty(), "被拒绝的操作不留下任何重放记录");
        // 拒绝后消息仍可被正常首次投递，不影响正常消费路径
        assertNotNull(broker.poll("g", "c1"), "PENDING 消息仍可正常首次投递");
    }

    @Test
    void replayDoesNotAffectOtherGroupsAndRemainsSameMessage() {
        Clock clock = new TickClock(3_000_000L, 100L);
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve("dead-replay-2.json")), clock);
        broker.registerGroup(new ConsumerGroupConfig("g1", "orders", 2,
                Duration.ofSeconds(10), noBackoff));
        broker.registerGroup(new ConsumerGroupConfig("g2", "orders", 2,
                Duration.ofSeconds(10), noBackoff));
        broker.enqueue(new Message("m-shared", "orders", "p", 1));

        for (String g : new String[]{"g1", "g2"}) {
            LeasedMessage l;
            while ((l = broker.poll(g, "c")) != null) {
                broker.acknowledgeFailure(l, true, "X");
            }
            assertEquals(MessageState.DEAD, broker.getMessageState(g, 0));
        }
        // 只重投 g1
        broker.replayDeadLetter("g1", 0, "ops", "compensate g1 only");

        System.out.println("【输入】同一条消息在 g1/g2 都死信，只重投 g1");
        LeasedMessage g1Lease = broker.poll("g1", "c");
        LeasedMessage g2Lease = broker.poll("g2", "c");
        System.out.println("【判定】g1 重放投递=" + (g1Lease != null)
                + " g2 重放投递=" + (g2Lease != null));

        assertNotNull(g1Lease);
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, g1Lease.deliveryKind());
        assertNull(g2Lease, "重投只作用于目标组，g2 仍是死信");
        assertEquals(MessageState.DEAD, broker.getMessageState("g2", 0));
        // 同一条消息的延续：消息 id、payload 都不变
        assertEquals("m-shared", g1Lease.message().id());
        assertEquals("p", g1Lease.message().payload());
    }
}
