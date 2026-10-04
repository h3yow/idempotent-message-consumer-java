package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ReplayRejectedException;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ReplayResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景六：消费进度重放（位点回退）。
 * 覆盖：回退后已成功的消息被重新投递（可区分是重放）、业务凭幂等键只生效一次、
 * 只影响目标组、非法回退（越界/非回退）以可区分原因拒绝且进度原样、
 * 回退不越过未确认消息（IN_FLIGHT 不被重置，进行中的确认照常生效）。
 */
class ReplayOffsetTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker newBroker(String name) {
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve(name + ".json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", 3,
                Duration.ofSeconds(30), noBackoff));
        broker.registerGroup(new ConsumerGroupConfig("g2", "orders", 3,
                Duration.ofSeconds(30), noBackoff));
        return broker;
    }

    /** 业务侧幂等表：同一 messageId 的效果只落一次，返回是否为重复生效。 */
    private static final class IdempotentEffect {
        final Set<String> applied = ConcurrentHashMap.newKeySet();
        final Map<String, AtomicInteger> handlerCalls = new ConcurrentHashMap<>();

        boolean apply(String messageId) {
            handlerCalls.computeIfAbsent(messageId, k -> new AtomicInteger()).incrementAndGet();
            return applied.add(messageId); // false = 幂等拦截，未重复生效
        }

        int effectCount(String messageId) {
            return applied.contains(messageId) ? 1 : 0;
        }
    }

    @Test
    void replayRedeliversSucceededMessagesButEffectStaysOnce() {
        LocalMessageBroker broker = newBroker("replay-basic");
        IdempotentEffect effect = new IdempotentEffect();
        int n = 5;
        for (int i = 0; i < n; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        // 正常消费完全部 5 条
        for (int i = 0; i < n; i++) {
            LeasedMessage lease = broker.poll("g", "c1");
            assertNotNull(lease);
            effect.apply(lease.message().id());
            broker.acknowledgeSuccess(lease);
        }
        assertEquals(n - 1L, broker.getCommittedOffset("g"));

        // 回退到 offset=1：2..4 应被重置为待重放
        ReplayResult result = broker.replayFrom("g", 1);
        System.out.println("【输入】replayFrom(g, 1)，回退前 committed=" + result.previousCommitted());
        System.out.println("【判定】" + broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.REPLAY_SCHEDULED).findFirst().orElseThrow().detail());
        System.out.println("【位点】回退后 committed=" + broker.getCommittedOffset("g"));

        assertEquals(4L, result.previousCommitted());
        assertEquals(1L, result.committed());
        assertEquals(3, result.resetToPending());
        assertEquals(1L, broker.getCommittedOffset("g"));
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 1),
                "回退点及之前的消息保持终态不动");
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 2));

        // 重放投递：2..4 再次到来，可区分是重放；业务幂等拦截，生效仍一次
        int replayed = 0;
        LeasedMessage lease;
        while ((lease = broker.poll("g", "c1")) != null) {
            System.out.println("【判定】重放投递 offset=" + lease.offset()
                    + " attemptNo=" + lease.attemptNo()
                    + " deliveryCause=" + lease.deliveryCause());
            assertEquals(LeasedMessage.CAUSE_REPLAY, lease.deliveryCause());
            assertEquals(2, lease.attemptNo(), "重放是同一条消息的延续，尝试序号续增");
            boolean firstEffect = effect.apply(lease.message().id());
            assertFalse(firstEffect, "重放不得造成第二次业务生效: " + lease.message().id());
            broker.acknowledgeSuccess(lease);
            replayed++;
        }
        assertEquals(3, replayed);
        assertEquals(n - 1L, broker.getCommittedOffset("g"), "重放完成后位点回到末端");
        for (int i = 0; i < n; i++) {
            assertEquals(1, effect.effectCount("m-" + i), "每条消息业务只生效一次");
            assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", i));
        }
        // 轨迹里能看到每次重放投递
        long replayDispatches = broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.REPLAY_DISPATCHED).count();
        assertEquals(3, replayDispatches);
        System.out.println("【依据】REPLAY_DISPATCHED 事件数=" + replayDispatches
                + " 每条消息 handlerCalls=" + effect.handlerCalls.values().stream()
                .mapToInt(AtomicInteger::get).sum() + "（含重放调用，但生效均幂等拦截）");
    }

    @Test
    void replayRejectionsAreDistinctAndLeaveProgressUntouched() {
        LocalMessageBroker broker = newBroker("replay-reject");
        // 入队 4 条（最大位点 3），只消费前 2 条：committed=1，留出“范围内前进”的空间
        for (int i = 0; i < 4; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        broker.acknowledgeSuccess(broker.poll("g", "c1"));
        broker.acknowledgeSuccess(broker.poll("g", "c1"));
        assertEquals(1L, broker.getCommittedOffset("g"));

        ReplayRejectedException range1 = assertThrows(ReplayRejectedException.class,
                () -> broker.replayFrom("g", -2));
        ReplayRejectedException range2 = assertThrows(ReplayRejectedException.class,
                () -> broker.replayFrom("g", 100));
        ReplayRejectedException notBackward1 = assertThrows(ReplayRejectedException.class,
                () -> broker.replayFrom("g", 1));
        ReplayRejectedException notBackward2 = assertThrows(ReplayRejectedException.class,
                () -> broker.replayFrom("g", 3));

        System.out.println("【输入】committed=1, maxOffset=3，分别请求 target=-2 / 100 / 1(等于) / 3(范围内前进)");
        System.out.println("【判定】-2 -> " + range1.code() + "；100 -> " + range2.code());
        System.out.println("【判定】1 -> " + notBackward1.code() + "；3 -> " + notBackward2.code());
        System.out.println("【位点】四次拒绝后 committed=" + broker.getCommittedOffset("g"));

        assertEquals(DecisionCode.REPLAY_REJECTED_RANGE, range1.code());
        assertEquals(DecisionCode.REPLAY_REJECTED_RANGE, range2.code());
        assertEquals(DecisionCode.REPLAY_REJECTED_NOT_BACKWARD, notBackward1.code());
        assertEquals(DecisionCode.REPLAY_REJECTED_NOT_BACKWARD, notBackward2.code());
        assertEquals(1L, broker.getCommittedOffset("g"), "被拒绝的回退不得留下半截进度");
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 1));
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 2));
        // 正常消费掉剩余两条，证明拒绝没有重置出任何消息
        LeasedMessage l2 = broker.poll("g", "c1");
        assertNotNull(l2);
        assertEquals(2L, l2.offset());
        assertEquals(LeasedMessage.CAUSE_NORMAL, l2.deliveryCause(), "被拒绝后不应有重放投递");
        broker.acknowledgeSuccess(l2);
        LeasedMessage l3 = broker.poll("g", "c1");
        assertNotNull(l3);
        broker.acknowledgeSuccess(l3);
        assertNull(broker.poll("g", "c1"));
        assertEquals(3L, broker.getCommittedOffset("g"));
    }

    @Test
    void replayAffectsOnlyTargetGroup() {
        LocalMessageBroker broker = newBroker("replay-isolation");
        broker.enqueue(new Message("m-0", "orders", "p0", 0));
        broker.enqueue(new Message("m-1", "orders", "p1", 1));
        for (String g : new String[]{"g", "g2"}) {
            while (true) {
                LeasedMessage lease = broker.poll(g, "c1");
                if (lease == null) break;
                broker.acknowledgeSuccess(lease);
            }
        }
        assertEquals(1L, broker.getCommittedOffset("g"));
        assertEquals(1L, broker.getCommittedOffset("g2"));

        broker.replayFrom("g", 0);
        System.out.println("【输入】仅对 g 回退到 0");
        System.out.println("【位点】g committed=" + broker.getCommittedOffset("g")
                + " g2 committed=" + broker.getCommittedOffset("g2"));

        assertEquals(0L, broker.getCommittedOffset("g"));
        assertEquals(1L, broker.getCommittedOffset("g2"), "其他组进度不受影响");
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g2", 1));
        assertNull(broker.poll("g2", "c1"), "g2 不应出现重放投递");
        LeasedMessage replayed = broker.poll("g", "c1");
        assertNotNull(replayed);
        assertEquals(LeasedMessage.CAUSE_REPLAY, replayed.deliveryCause());
    }

    @Test
    void replayDoesNotCrossInFlightMessagesAndOngoingAckIsHonored() {
        LocalMessageBroker broker = newBroker("replay-inflight");
        for (int i = 0; i < 4; i++) {
            broker.enqueue(new Message("m-" + i, "orders", "p" + i, i));
        }
        // offset 0、1 成功；offset 2 租出未确认（IN_FLIGHT）；offset 3 待处理
        broker.acknowledgeSuccess(broker.poll("g", "c1"));
        broker.acknowledgeSuccess(broker.poll("g", "c1"));
        LeasedMessage inFlight = broker.poll("g", "c-slow");
        assertNotNull(inFlight);
        assertEquals(2L, inFlight.offset());
        assertEquals(1L, broker.getCommittedOffset("g"));

        // committed=1，无法回退到 <1 的同时覆盖 offset=2；先显式推进位点不可能（缺口）。
        // 这里直接回退到 0：区间内只有 offset=1（SUCCEEDED），IN_FLIGHT 的 offset=2 不在区间内。
        ReplayResult r1 = broker.replayFrom("g", 0);
        assertEquals(1, r1.resetToPending());
        assertEquals(0, r1.leftInFlight());

        // 进行中的确认照常生效，不被回退覆盖
        broker.acknowledgeSuccess(inFlight);
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 2));

        // 消费掉重放的 offset=1 与后续消息，位点连续推进、不越过任何未确认消息
        LeasedMessage replayed = broker.poll("g", "c1");
        assertNotNull(replayed);
        assertEquals(1L, replayed.offset());
        assertEquals(LeasedMessage.CAUSE_REPLAY, replayed.deliveryCause());
        broker.acknowledgeSuccess(replayed);
        LeasedMessage tail = broker.poll("g", "c1");
        assertNotNull(tail);
        assertEquals(3L, tail.offset());
        broker.acknowledgeSuccess(tail);

        System.out.println("【输入】offset=2 IN_FLIGHT 期间回退到 0，随后慢消费者确认");
        System.out.println("【位点】最终 committed=" + broker.getCommittedOffset("g"));
        System.out.println("【判定】offset=2 state=" + broker.getMessageState("g", 2)
                + "（进行中的 ack 未被覆盖）");
        assertEquals(3L, broker.getCommittedOffset("g"));
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 2));
        assertNull(broker.poll("g", "c1"));
    }
}
