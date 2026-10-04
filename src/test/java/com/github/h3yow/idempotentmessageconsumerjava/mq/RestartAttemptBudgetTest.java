package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景七：重启与尝试额度的一致性。
 * 重启只把未确认消息重新投出，已消耗的尝试次数继续算数：
 * 不因重启多出投递机会、不突破 maxAttempts；到上限还失败的消息老老实实进死信。
 */
class RestartAttemptBudgetTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker startBroker(int maxAttempts) {
        LocalMessageBroker broker = new LocalMessageBroker(
                new FileBrokerStore(tmp.resolve("state.json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofSeconds(30), noBackoff));
        return broker;
    }

    @Test
    void restartDoesNotGrantExtraAttemptsBeyondMaxAttempts() {
        // maxAttempts=2：第 1 次失败重试，第 2 次租出后“宕机”不确认
        LocalMessageBroker first = startBroker(2);
        first.enqueue(new Message("m-b", "orders", "crash-at-last-attempt", 1));
        LeasedMessage a1 = first.poll("g", "c1");
        assertNotNull(a1);
        assertEquals(1, a1.attemptNo());
        first.acknowledgeFailure(a1, true, "REMOTE_500");
        LeasedMessage a2 = first.poll("g", "c1");
        assertNotNull(a2);
        assertEquals(2, a2.attemptNo());
        // 宕机：不确认，直接丢弃实例
        System.out.println("【输入】maxAttempts=2，第 2 次尝试租出后宕机（未确认）");
        System.out.println("【位点】宕机前 committed=" + first.getCommittedOffset("g")
                + " attemptCount=" + first.getAttemptCount("g", 0));

        // 重启：未确认租约被回收，但尝试额度已耗尽 —— 不得发放第 3 次租约
        LocalMessageBroker second = startBroker(2);
        LeasedMessage third = second.poll("g", "c-new");

        List<TraceEvent> dispatches = second.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.DISPATCHED)
                .toList();
        DeadLetter dl = second.getDeadLetters().isEmpty()
                ? null : second.getDeadLetters().get(0);
        System.out.println("【判定】重启后 poll=" + third
                + " 新实例投递事件数=" + dispatches.size());
        System.out.println("【判定】死信=" + (dl == null ? "无" : dl.cause()
                + " attempts=" + dl.attempts().size()));
        System.out.println("【位点】committed=" + second.getCommittedOffset("g")
                + " state=" + second.getMessageState("g", 0));
        System.out.println("【依据】" + second.lastTraceFor("g", "m-b").code()
                + " | " + second.lastTraceFor("g", "m-b").detail());

        assertNull(third, "尝试额度已耗尽，重启不得多出投递机会");
        assertTrue(dispatches.isEmpty(), "重启后不得出现第 3 次投递");
        assertEquals(MessageState.DEAD, second.getMessageState("g", 0));
        assertNotNull(dl);
        assertEquals("MAX_ATTEMPTS_REACHED", dl.cause());
        assertEquals(2, second.getAttemptCount("g", 0), "尝试次数保持 2，不突破上限");
        assertEquals(DecisionCode.NACK_RETRY_EXHAUSTED,
                second.lastTraceFor("g", "m-b").code());
        assertEquals(0L, second.getCommittedOffset("g"), "死信是终态，位点越过该消息");
        assertNull(second.poll("g", "c-new"), "死信后不得再投递");
    }

    @Test
    void restartKeepsRemainingBudgetAndExhaustsExactly() {
        // maxAttempts=3：第 1 次失败，第 2 次租出后宕机；重启后应恰好只剩 1 次机会
        LocalMessageBroker first = startBroker(3);
        first.enqueue(new Message("m-k", "orders", "crash-mid-budget", 1));
        LeasedMessage a1 = first.poll("g", "c1");
        first.acknowledgeFailure(a1, true, "REMOTE_500");
        LeasedMessage a2 = first.poll("g", "c1");
        assertEquals(2, a2.attemptNo());
        // 宕机

        LocalMessageBroker second = startBroker(3);
        LeasedMessage a3 = second.poll("g", "c-new");
        System.out.println("【输入】maxAttempts=3，第 2 次尝试未确认即宕机，重启后继续");
        System.out.println("【判定】重启后投递 attemptNo=" + (a3 == null ? null : a3.attemptNo()));
        assertNotNull(a3, "额度未耗尽，重启后应继续投递");
        assertEquals(3, a3.attemptNo(), "已消耗次数继续算数：这是第 3 次而非重置后的第 1 次");

        // 第 3 次仍失败：到达上限，进死信，之后绝无第 4 次
        second.acknowledgeFailure(a3, true, "REMOTE_500");
        System.out.println("【位点】第 3 次失败后 state=" + second.getMessageState("g", 0)
                + " committed=" + second.getCommittedOffset("g"));
        System.out.println("【依据】" + second.lastTraceFor("g", "m-k").code()
                + " | " + second.lastTraceFor("g", "m-k").detail());
        assertEquals(MessageState.DEAD, second.getMessageState("g", 0));
        assertNull(second.poll("g", "c-new"));
        assertEquals(1, second.getDeadLetters().size());
        assertEquals(3, second.getAttemptCount("g", 0));

        // 再重启一次：死信是终态，不得复活
        LocalMessageBroker third = startBroker(3);
        assertNull(third.poll("g", "c-new"));
        assertEquals(MessageState.DEAD, third.getMessageState("g", 0));
        assertEquals(1, third.getDeadLetters().size());
        System.out.println("【判定】二次重启后死信保持终态，不再投递");
    }
}
