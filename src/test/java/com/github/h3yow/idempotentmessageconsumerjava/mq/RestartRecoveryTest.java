package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景四：服务重启恢复。
 * 重启时已成功的消息保持终态不重投；未确认（IN_FLIGHT）的消息被重置为可重新投递，
 * 重新消费后只生效一次，且轨迹里保留“因重启未确认”的 LEASE_TIMEOUT 记录。
 */
class RestartRecoveryTest {

    @TempDir
    Path tmp;

    private LocalMessageBroker startBroker(String instanceName) {
        LocalMessageBroker broker = new LocalMessageBroker(
                new FileBrokerStore(tmp.resolve("state.json")));
        broker.registerGroup(ConsumerGroupConfig.of("g", "orders"));
        return broker;
    }

    @Test
    void unackedMessageIsRedeliveredAfterRestartWithoutDoubleEffect() {
        // ---- 第一次启动：成功处理 offset=0；租出 offset=1 后“宕机”（不确认）----
        LocalMessageBroker first = startBroker("first");
        first.enqueue(new Message("m-0", "orders", "done-before-restart", 1));
        first.enqueue(new Message("m-1", "orders", "inflight-at-crash", 2));
        first.enqueue(new Message("m-2", "orders", "pending-at-crash", 3));

        LeasedMessage l0 = first.poll("g", "c-old");
        first.acknowledgeSuccess(l0);
        LeasedMessage l1 = first.poll("g", "c-old");
        assertNotNull(l1);
        assertEquals(1L, l1.offset());
        // 模拟宕机：不调用任何确认，直接丢弃 broker 实例（租约快照已落盘）

        System.out.println("【输入】重启前: offset=0 SUCCEEDED, offset=1 IN_FLIGHT(未确认), offset=2 PENDING");
        System.out.println("【位点】重启前 committed=" + first.getCommittedOffset("g"));
        assertEquals(0L, first.getCommittedOffset("g"));
        assertEquals(MessageState.IN_FLIGHT, first.getMessageState("g", 1));

        // ---- 第二次启动：同一状态文件，重新注册组 ----
        LocalMessageBroker second = startBroker("second");

        TraceEvent recovery = second.getTrace().stream()
                .filter(e -> e.code() == DecisionCode.RECOVERED_ON_STARTUP)
                .findFirst().orElse(null);
        System.out.println("【判定】恢复事件: " + (recovery == null ? null : recovery.detail()));
        assertNotNull(recovery);
        assertEquals(0L, second.getCommittedOffset("g"), "重启不改变已提交位点");
        assertEquals(MessageState.SUCCEEDED, second.getMessageState("g", 0),
                "已成功状态不得被污染或重投");

        // 已成功消息不再投递；第一条拿到的必须是被回收的 offset=1
        LeasedMessage r1 = second.poll("g", "c-new");
        assertNotNull(r1);
        System.out.println("【判定】重启后首次投递: offset=" + r1.offset()
                + " messageId=" + r1.message().id() + " attemptNo=" + r1.attemptNo());
        assertEquals(1L, r1.offset());
        assertEquals(2, r1.attemptNo(), "重启重投应计为第 2 次尝试");
        second.acknowledgeSuccess(r1);

        LeasedMessage r2 = second.poll("g", "c-new");
        assertNotNull(r2);
        assertEquals(2L, r2.offset());
        second.acknowledgeSuccess(r2);
        LeasedMessage r3 = second.poll("g", "c-new");
        assertNull(r3, "全部终态后队列应为空");

        List<AttemptRecord> attemptsOf1 = second.getAttempts("g", 1);
        System.out.println("【依据】offset=1 的尝试轨迹:");
        for (AttemptRecord a : attemptsOf1) {
            System.out.println("    attemptNo=" + a.attemptNo() + " result=" + a.result()
                    + " reason=" + a.reason() + " detail=" + a.detail());
        }
        System.out.println("【位点】重启恢复消费完成后 committed=" + second.getCommittedOffset("g"));

        assertEquals(2L, second.getCommittedOffset("g"));
        assertEquals(MessageState.SUCCEEDED, second.getMessageState("g", 1));
        assertEquals(2, attemptsOf1.size());
        // 第一条轨迹是“重启时发现未确认”的可追溯原因
        assertEquals(AttemptResult.LEASE_TIMEOUT, attemptsOf1.get(0).result());
        assertEquals("RECOVERED_AFTER_RESTART_UNACKED", attemptsOf1.get(0).reason());
        assertEquals(AttemptResult.SUCCESS, attemptsOf1.get(1).result());
        // offset=0 的成功轨迹不被重启追加任何记录
        assertEquals(1, second.getAttempts("g", 0).size());
        assertTrue(second.getDeadLetters().isEmpty());

        // ---- 第三次启动：没有未确认消息，不应产生恢复事件，且无消息可投 ----
        LocalMessageBroker third = startBroker("third");
        boolean noRecovery = third.getTrace().stream()
                .noneMatch(e -> e.code() == DecisionCode.RECOVERED_ON_STARTUP);
        System.out.println("【判定】第三次启动（无未确认消息）恢复事件数=0: " + noRecovery);
        assertTrue(noRecovery);
        assertNull(third.poll("g", "c-new"));
        assertEquals(2L, third.getCommittedOffset("g"));
    }
}
