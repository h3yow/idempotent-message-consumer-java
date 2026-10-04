package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.RedeliverRejectedException;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景五：死信人工重投（补偿）。
 * 覆盖：重投是同一条消息的延续（attemptNo 续增、轨迹可区分人为重投）、
 * 原死信记录保留、业务生效仍最多一次、重投后失败仍受重试上限约束（只多一次机会）、
 * 两类拒绝原因可区分且状态不变。
 */
class DeadLetterRedeliverTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker newBroker(String name, int maxAttempts) {
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve(name + ".json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofSeconds(30), noBackoff));
        return broker;
    }

    /** 让消息以可重试失败耗尽 maxAttempts 次尝试，进入死信。 */
    private void driveToDead(LocalMessageBroker broker, int maxAttempts) {
        for (int i = 0; i < maxAttempts; i++) {
            LeasedMessage lease = broker.poll("g", "c1");
            assertNotNull(lease, "第 " + (i + 1) + " 次投递应发生");
            broker.acknowledgeFailure(lease, true, "REMOTE_500");
        }
        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));
        assertNull(broker.poll("g", "c1"), "死信后不应再自动投递");
    }

    @Test
    void redeliveredDeadLetterContinuesSameMessageAndSucceedsOnce() {
        LocalMessageBroker broker = newBroker("redeliver-ok", 3);
        broker.enqueue(new Message("m-d", "orders", "compensate-me", 1));
        driveToDead(broker, 3);
        assertEquals(1, broker.getDeadLetters().size());

        // 人工重投：审计轨迹必须能看出“人为放回”
        long offset = broker.redeliverDeadLetter("g", "m-d");
        TraceEvent redelivered = broker.lastTraceFor("g", "m-d");
        System.out.println("【输入】redeliverDeadLetter(g, m-d)，原死信保留");
        System.out.println("【判定】" + redelivered.code() + " | " + redelivered.detail());

        assertEquals(0L, offset);
        assertEquals(DecisionCode.REDELIVERED_FROM_DEAD_LETTER, redelivered.code());
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 0));

        // 重投的投递：attemptNo 续增为 4，deliveryCause 可区分
        AtomicInteger businessEffect = new AtomicInteger();
        LeasedMessage lease = broker.poll("g", "c-ops");
        assertNotNull(lease);
        System.out.println("【判定】重投后投递 attemptNo=" + lease.attemptNo()
                + " deliveryCause=" + lease.deliveryCause());
        assertEquals(4, lease.attemptNo(), "重投是同一条消息的延续，尝试序号继续递增");
        assertEquals(LeasedMessage.CAUSE_REDELIVER, lease.deliveryCause());
        assertEquals(DecisionCode.REDELIVER_DISPATCHED, broker.lastTraceFor("g", "m-d").code());

        businessEffect.incrementAndGet(); // 前 3 次都失败，业务从未生效；这是第一次生效
        broker.acknowledgeSuccess(lease);

        System.out.println("【位点】committed=" + broker.getCommittedOffset("g")
                + " state=" + broker.getMessageState("g", 0));
        System.out.println("【依据】死信记录数=" + broker.getDeadLetters().size()
                + " 业务生效次数=" + businessEffect.get());

        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
        assertEquals(1, businessEffect.get(), "全组业务生效最多一次");
        assertEquals(1, broker.getDeadLetters().size(), "原死信记录不得被重投抹掉");
        DeadLetter dl = broker.getDeadLetters().get(0);
        assertEquals("MAX_ATTEMPTS_REACHED", dl.cause());
        assertEquals(3, dl.attempts().size(), "死信记录保留重投前的全部尝试轨迹");
        assertNull(broker.poll("g", "c1"), "成功后不得再投递");
    }

    @Test
    void redeliveredMessageFailsAgainAndStaysBoundedByMaxAttempts() {
        LocalMessageBroker broker = newBroker("redeliver-fail", 2);
        broker.enqueue(new Message("m-f", "orders", "still-bad", 1));
        driveToDead(broker, 2);

        broker.redeliverDeadLetter("g", "m-f");
        LeasedMessage lease = broker.poll("g", "c-ops");
        assertNotNull(lease);
        assertEquals(3, lease.attemptNo(), "重投恰好多给一次机会");
        broker.acknowledgeFailure(lease, true, "REMOTE_500");

        System.out.println("【输入】重投后第 3 次尝试仍失败（maxAttempts=2，重投1次）");
        System.out.println("【位点】committed=" + broker.getCommittedOffset("g")
                + " state=" + broker.getMessageState("g", 0));
        System.out.println("【判定】再poll=" + broker.poll("g", "c1"));
        System.out.println("【依据】" + broker.lastTraceFor("g", "m-f").code()
                + " | " + broker.lastTraceFor("g", "m-f").detail());

        // 重投只多给一次机会：再次失败立即回到死信，不会无限重试
        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));
        assertNull(broker.poll("g", "c1"), "重投额度用完后不得再有下一次投递");
        assertEquals(DecisionCode.NACK_RETRY_EXHAUSTED, broker.lastTraceFor("g", "m-f").code());
        assertEquals(2, broker.getDeadLetters().size(), "再次耗尽追加新死信，原记录仍保留");
        assertEquals(2, broker.getDeadLetters().get(0).attempts().size());
        assertEquals(3, broker.getDeadLetters().get(1).attempts().size(),
                "新死信包含重投后的完整轨迹");

        // 第二次人工重投仍然可行，但仍只多一次机会
        broker.redeliverDeadLetter("g", "m-f");
        LeasedMessage again = broker.poll("g", "c-ops");
        assertNotNull(again);
        assertEquals(4, again.attemptNo());
        broker.acknowledgeFailure(again, true, "REMOTE_500");
        assertEquals(MessageState.DEAD, broker.getMessageState("g", 0));
        assertNull(broker.poll("g", "c1"));
        assertEquals(3, broker.getDeadLetters().size());
    }

    @Test
    void redeliverRejectionsAreDistinctAndLeaveStateUntouched() {
        LocalMessageBroker broker = newBroker("redeliver-reject", 3);
        broker.enqueue(new Message("m-ok", "orders", "fine", 1));
        broker.enqueue(new Message("m-pending", "orders", "waiting", 2));

        // 成功终态的消息不可重投
        LeasedMessage lease = broker.poll("g", "c1");
        broker.acknowledgeSuccess(lease);

        RedeliverRejectedException notDead = assertThrows(RedeliverRejectedException.class,
                () -> broker.redeliverDeadLetter("g", "m-ok"));
        RedeliverRejectedException unknown = assertThrows(RedeliverRejectedException.class,
                () -> broker.redeliverDeadLetter("g", "no-such-id"));

        System.out.println("【输入】对 SUCCEEDED 消息与不存在的 messageId 分别重投");
        System.out.println("【判定】非死信 -> " + notDead.code());
        System.out.println("【判定】未知消息 -> " + unknown.code());
        System.out.println("【位点】committed=" + broker.getCommittedOffset("g")
                + " m-ok=" + broker.getMessageState("g", 0)
                + " m-pending=" + broker.getMessageState("g", 1));

        assertEquals(DecisionCode.REDELIVER_REJECTED_NOT_DEAD, notDead.code());
        assertEquals(DecisionCode.REDELIVER_REJECTED_UNKNOWN_MESSAGE, unknown.code());
        // 被拒绝后状态原样
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g", 0));
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 1));
        assertEquals(0L, broker.getCommittedOffset("g"));
        assertTrue(broker.getDeadLetters().isEmpty());

        // 待处理消息同样不可重投（只有死信终态可以）
        RedeliverRejectedException pending = assertThrows(RedeliverRejectedException.class,
                () -> broker.redeliverDeadLetter("g", "m-pending"));
        assertEquals(DecisionCode.REDELIVER_REJECTED_NOT_DEAD, pending.code());
        assertEquals(MessageState.PENDING, broker.getMessageState("g", 1));

        List<TraceEvent> rejects = broker.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.REDELIVER_REJECTED_NOT_DEAD
                        || e.code() == DecisionCode.REDELIVER_REJECTED_UNKNOWN_MESSAGE)
                .toList();
        assertEquals(3, rejects.size(), "每次拒绝都必须留审计轨迹");
    }
}
