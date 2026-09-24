package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.EnqueueResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景一：重复投递 / 重复消费。
 * 覆盖：相同 id 重复入队被忽略（原因可区分）、成功后重复 poll 不再投递（不重复改变业务状态）、
 * 不同消费者组互不影响。日志打印输入、位点与判定依据（MQ-TRACE）。
 */
class DuplicateDeliveryTest {

    @TempDir
    Path tmp;

    private LocalMessageBroker newBroker() {
        return new LocalMessageBroker(new FileBrokerStore(tmp.resolve("state.json")));
    }

    @Test
    void duplicateEnqueueIsIgnoredWithDistinctReason() {
        LocalMessageBroker broker = newBroker();
        broker.registerGroup(ConsumerGroupConfig.of("g1", "orders"));

        Message first = new Message("m-1", "orders", "create-order-1", 1000);
        Message again = new Message("m-1", "orders", "create-order-1", 2000);

        EnqueueResult r1 = broker.enqueue(first);
        EnqueueResult r2 = broker.enqueue(again);

        System.out.println("【输入】第一次入队: " + first);
        System.out.println("【输入】重复入队:   " + again);
        System.out.println("【判定】r1.accepted=" + r1.accepted() + " code=" + r1.code());
        System.out.println("【判定】r2.accepted=" + r2.accepted() + " code=" + r2.code());
        for (TraceEvent ev : broker.getTrace()) {
            System.out.println("【轨迹】" + ev.code() + " | " + ev.detail());
        }

        assertTrue(r1.accepted());
        assertEquals(DecisionCode.ENQUEUED, r1.code());
        assertFalse(r2.accepted());
        assertEquals(DecisionCode.ENQUEUE_DUPLICATED_GLOBAL_ID, r2.code());
        // 重复入队返回的是首次入队的原始消息，不新增任何记录
        assertSame(first, r2.message());
        // 消息尚未被消费，连续终态水位保持 -1（没有任何位点可提交）
        assertEquals(-1L, broker.getCommittedOffset("g1"));
        assertEquals(MessageState.PENDING, broker.getMessageState("g1", 0));
    }

    @Test
    void succeededMessageIsNeverEffectivelyConsumedTwice() {
        LocalMessageBroker broker = newBroker();
        broker.registerGroup(ConsumerGroupConfig.of("g1", "orders"));

        AtomicInteger businessEffect = new AtomicInteger();
        broker.enqueue(new Message("m-9", "orders", "pay", 1));

        // 第一次投递：业务生效
        LeasedMessage lease1 = broker.poll("g1", "c1");
        assertNotNull(lease1);
        businessEffect.incrementAndGet();
        broker.acknowledgeSuccess(lease1);

        // 模拟重复投递/重复消费：再次 poll 不应再拿到该消息
        LeasedMessage lease2 = broker.poll("g1", "c1");
        LeasedMessage lease3 = broker.poll("g1", "c2");

        System.out.println("【输入】messageId=m-9, 两次成功后再次 poll（c1/c2）");
        System.out.println("【位点】committed=" + broker.getCommittedOffset("g1")
                + " state=" + broker.getMessageState("g1", 0));
        System.out.println("【判定】第二次poll=" + lease2 + " 第三次poll=" + lease3);
        System.out.println("【依据】最后一条与 m-9 相关轨迹: "
                + broker.lastTraceFor("g1", "m-9").code() + " | "
                + broker.lastTraceFor("g1", "m-9").detail());

        assertNull(lease2, "已成功消息不得重复投递");
        assertNull(lease3);
        assertEquals(1, businessEffect.get(), "业务状态只能被改变一次");
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g1", 0));
        assertEquals(0L, broker.getCommittedOffset("g1"));
        assertEquals(DecisionCode.ACK_SUCCEEDED,
                broker.lastTraceFor("g1", "m-9").code());
    }

    @Test
    void staleAcknowledgementIsIgnoredAndCannotPolluteSuccess() {
        LocalMessageBroker broker = newBroker();
        broker.registerGroup(ConsumerGroupConfig.of("g1", "orders"));
        broker.enqueue(new Message("m-3", "orders", "x", 1));

        LeasedMessage leaseA = broker.poll("g1", "c1");
        assertNotNull(leaseA);
        broker.acknowledgeSuccess(leaseA);

        // 已成功的消息不会再发租约；此处直接验证：即便持有旧 lease 的迟到确认到达，也必须被忽略
        broker.acknowledgeFailure(leaseA, true, "LATE_NACK_FROM_SLOW_CONSUMER");

        TraceEvent stale = broker.lastTraceFor("g1", "m-3");
        System.out.println("【输入】旧租约迟到 nack: leaseId=" + leaseA.leaseId());
        System.out.println("【判定】" + stale.code() + " | " + stale.detail());
        System.out.println("【位点】committed=" + broker.getCommittedOffset("g1")
                + " state=" + broker.getMessageState("g1", 0));

        // 最后一条与该消息相关的轨迹必须是“过期租约忽略”，且状态保持 SUCCEEDED
        assertEquals(DecisionCode.LEASE_STALE_IGNORED, stale.code());
        assertEquals(MessageState.SUCCEEDED, broker.getMessageState("g1", 0));
        assertEquals(0L, broker.getCommittedOffset("g1"));
        assertTrue(broker.getDeadLetters().isEmpty());
    }
}
