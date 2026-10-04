package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryKind;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ReplayRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 运维场景四：重启恢复与尝试额度的账对齐。
 * 覆盖：
 * 1) 未确认消息在崩溃前已占满最后一次尝试额度（maxAttempts=3，前 2 次失败退避、第 3 次租出后宕机），
 *    重启回收只能计 LEASE_TIMEOUT 并直接进死信——重启不多给投递机会、不突破上限、不无限重投；
 * 2) 仍有剩余额度的未确认消息重启后正常重投，消耗掉的尝试次数继续算数；
 * 3) 死信重投之后、重放投递确认之前宕机，重启后重放轮次与 attemptNo 延续，成功后业务只生效一次，
 *    且再次失败仍然死信（重放也不赠送额度）。
 */
class ReplayRestartBudgetTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker start(String maxAttemptsFile, int maxAttempts) {
        LocalMessageBroker broker = new LocalMessageBroker(
                new FileBrokerStore(tmp.resolve(maxAttemptsFile)));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", maxAttempts,
                Duration.ofHours(1), noBackoff));
        return broker;
    }

    @Test
    void restartDoesNotGrantExtraAttemptWhenBudgetAlreadyExhausted() {
        String file = "restart-budget.json";
        // ---- 第一次启动：前 2 次可重试失败，第 3 次租出后“宕机”不确认 ----
        LocalMessageBroker first = start(file, 3);
        first.enqueue(new Message("m-exhaust", "orders", "p", 1));
        LeasedMessage a1 = first.poll("g", "c-old");
        first.acknowledgeFailure(a1, true, "FAIL_1");
        LeasedMessage a2 = first.poll("g", "c-old");
        first.acknowledgeFailure(a2, true, "FAIL_2");
        LeasedMessage a3 = first.poll("g", "c-old");
        assertNotNull(a3);
        assertEquals(3, a3.attemptNo());
        // 宕机：不确认
        System.out.println("【输入】maxAttempts=3：前2次失败，第3次租出 attemptNo=3 后宕机（未确认）");
        System.out.println("【位点】崩溃前 committed=" + first.getCommittedOffset("g"));
        assertEquals(-1L, first.getCommittedOffset("g"));

        // ---- 重启：回收第 3 次租约，额度已耗尽，必须直接死信，不得再次投递 ----
        LocalMessageBroker second = start(file, 3);
        System.out.println("【位点】重启后 committed=" + second.getCommittedOffset("g")
                + " state=" + second.getMessageState("g", 0));
        LeasedMessage redelivered = second.poll("g", "c-new");

        System.out.println("【判定】重启后 poll=" + (redelivered == null
                ? "null（额度耗尽，直接死信，不多给投递机会）" : "又拿到了消息（错误）"));

        assertNull(redelivered, "已占满 maxAttempts 的未确认消息，重启后不得再投递");
        assertEquals(MessageState.DEAD, second.getMessageState("g", 0));
        assertEquals(0L, second.getCommittedOffset("g"), "死信是终态，水线越过它前进");
        List<DeadLetter> dead = second.getDeadLetters();
        assertEquals(1, dead.size());
        assertEquals("MAX_ATTEMPTS_REACHED", dead.get(0).cause());
        assertEquals(3, dead.get(0).attempts().size());
        List<AttemptRecord> attempts = second.getAttempts("g", 0);
        assertEquals(3, attempts.size(), "尝试记录恰好 3 条：2 次失败 + 1 次重启回收超时");
        assertEquals(AttemptResult.LEASE_TIMEOUT, attempts.get(2).result());
        assertEquals("RECOVERED_AFTER_RESTART_UNACKED", attempts.get(2).reason());
        TraceEvent recovery = second.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.RECOVERED_ON_STARTUP).findFirst().orElseThrow();
        System.out.println("【依据】" + recovery.code() + " | " + recovery.detail());
        assertTrue(recovery.detail().contains("exhausted the attempt budget"),
                "恢复轨迹必须交代“额度耗尽直接死信”的判定依据");
    }

    @Test
    void restartWithRemainingBudgetRedeliversAndKeepsCountingConsumedAttempts() {
        String file = "restart-remaining.json";
        LocalMessageBroker first = start(file, 3);
        first.enqueue(new Message("m-left", "orders", "p", 1));
        LeasedMessage a1 = first.poll("g", "c-old");
        first.acknowledgeFailure(a1, true, "FAIL_1");
        LeasedMessage a2 = first.poll("g", "c-old");
        assertNotNull(a2);
        assertEquals(2, a2.attemptNo());
        // 第 2 次租出后宕机：还剩 1 次自动额度
        System.out.println("【输入】第2次尝试租出后宕机（maxAttempts=3，剩余自动额度=1）");

        LocalMessageBroker second = start(file, 3);
        LeasedMessage r2 = second.poll("g", "c-new");
        assertNotNull(r2);
        System.out.println("【判定】重启后投递 attemptNo=" + r2.attemptNo() + "（期望 3，历史次数继续算数）");
        assertEquals(3, r2.attemptNo(), "重启不重置 attemptNo 计数");
        second.acknowledgeFailure(r2, true, "FAIL_3");

        // 第 3 次也失败 -> 立即死信，没有第 4 次
        LeasedMessage r3 = second.poll("g", "c-new");
        System.out.println("【判定】第3次失败后再 poll=" + (r3 == null ? "null（已死信）" : r3.attemptNo()));
        assertNull(r3);
        assertEquals(MessageState.DEAD, second.getMessageState("g", 0));
        assertEquals(3, second.getAttempts("g", 0).size());
        assertEquals("MAX_ATTEMPTS_REACHED", second.getDeadLetters().get(0).cause());
    }

    @Test
    void deadLetterReplayRoundSurvivesRestartAndStillBounded() {
        String file = "restart-replay.json";
        LocalMessageBroker first = start(file, 2);
        first.enqueue(new Message("m-rp", "orders", "p", 1));
        LeasedMessage l;
        while ((l = first.poll("g", "c")) != null) {
            first.acknowledgeFailure(l, true, "X");
        }
        assertEquals(MessageState.DEAD, first.getMessageState("g", 0));
        ReplayRecord replay = first.replayDeadLetter("g", 0, "ops-dave", "compensate then crash");
        LeasedMessage replayLease = first.poll("g", "c");
        assertNotNull(replayLease);
        assertEquals(3, replayLease.attemptNo());
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, replayLease.deliveryKind());
        // 重放投递确认前宕机
        System.out.println("【输入】死信重放后、重放投递(attemptNo=3)确认前宕机");
        System.out.println("【依据】重放记录已持久化: no=" + replay.replayNo());

        // 重启：该消息 attemptCount=3 >= maxAttempts=2，重放投递也不赠送额度：
        // 回收重放租约后直接死信（带 replayNo=1），不会无限重投
        LocalMessageBroker second = start(file, 2);
        LeasedMessage after = second.poll("g", "c");
        System.out.println("【判定】重启后 poll=" + (after == null ? "null" : after.attemptNo()));
        assertNull(after, "重放轮次同样受上限约束，重启不救回已耗尽的额度");
        assertEquals(MessageState.DEAD, second.getMessageState("g", 0));
        List<DeadLetter> deadLetters = second.getDeadLetters();
        // 原始死信(replayNo=0) + 重放后再次死信(replayNo=1)
        assertEquals(2, deadLetters.size());
        assertEquals(1, deadLetters.get(1).replayNo());
        assertEquals("ops-dave", deadLetters.get(1).operator());
        List<AttemptRecord> attempts = second.getAttempts("g", 0);
        AttemptRecord recovered = attempts.get(attempts.size() - 1);
        assertEquals(AttemptResult.LEASE_TIMEOUT, recovered.result());
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, recovered.deliveryKind(),
                "回收记录必须保留“这是重放轮次内的租约”这一事实");
        assertEquals(1, second.getReplays("g", 0).size(), "重放审计记录跨重启保留");

        // 人工再补偿一次，成功收尾，业务仍然只生效一次
        second.replayDeadLetter("g", 0, "ops-dave", "compensate again after restart");
        LeasedMessage finalLease = second.poll("g", "c");
        assertNotNull(finalLease);
        assertEquals(4, finalLease.attemptNo());
        assertEquals(2, finalLease.replayNo());
        second.acknowledgeSuccess(finalLease);
        assertEquals(MessageState.SUCCEEDED, second.getMessageState("g", 0));
        assertTrue(second.hasBusinessEffectTaken("g", 0));
        assertNull(second.poll("g", "c"));
    }
}
