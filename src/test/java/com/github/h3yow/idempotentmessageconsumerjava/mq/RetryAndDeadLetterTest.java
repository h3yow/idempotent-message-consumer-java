package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.TraceEvent;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景二：可重试失败按退避重试，超过上限进入死信；不可重试失败一次即死信。
 * 使用固定时钟（每次调用推进 100ms）与零退避，结论确定可复现。
 */
class RetryAndDeadLetterTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker brokerWithMaxAttempts(String group, int maxAttempts) {
        Clock clock = new TickClock(1_000_000L, 100L);
        LocalMessageBroker broker =
                new LocalMessageBroker(new FileBrokerStore(tmp.resolve("s-" + group + ".json")), clock);
        broker.registerGroup(new ConsumerGroupConfig(group, "orders", maxAttempts,
                Duration.ofSeconds(10), noBackoff));
        return broker;
    }

    /** 每次 millis() 调用都向前推进固定步长的时钟，保证时间断言确定。 */
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
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis()); }
    }

    @Test
    void retryableFailuresRetryUpToMaxAttemptsThenDeadLetter() {
        LocalMessageBroker broker = brokerWithMaxAttempts("g-retry", 3);
        broker.enqueue(new Message("m-r", "orders", "flaky", 1));

        // 每次都以可重试失败确认，直到耗尽 3 次尝试
        int polls = 0;
        LeasedMessage lease;
        while ((lease = broker.poll("g-retry", "c1")) != null) {
            polls++;
            System.out.println("【输入】第 " + lease.attemptNo() + " 次投递 offset="
                    + lease.offset() + " messageId=" + lease.message().id());
            broker.acknowledgeFailure(lease, true, "REMOTE_500");
            System.out.println("【位点】committed=" + broker.getCommittedOffset("g-retry")
                    + " state=" + broker.getMessageState("g-retry", 0));
        }

        List<AttemptRecord> attempts = broker.getAttempts("g-retry", 0);
        DeadLetter dl = broker.getDeadLetters().get(0);

        System.out.println("【判定】总投递次数=" + polls + "（期望 3）");
        for (TraceEvent ev : broker.getTrace()) {
            if (ev.group() != null) {
                System.out.println("【轨迹】" + ev.code() + " offset=" + ev.offset() + " | " + ev.detail());
            }
        }
        System.out.println("【依据】死信 cause=" + dl.cause() + " 尝试数=" + dl.attempts().size()
                + " 原始payload=" + dl.message().payload());

        assertEquals(3, polls, "最大尝试次数为 3");
        assertEquals(3, attempts.size());
        assertTrue(attempts.stream().allMatch(a -> a.result() == AttemptResult.RETRYABLE_FAILURE));
        assertEquals(1, broker.getDeadLetters().size());
        assertEquals("MAX_ATTEMPTS_REACHED", dl.cause(), "耗尽原因必须可区分");
        assertEquals("flaky", dl.message().payload(), "死信必须保留原始消息");
        assertEquals(MessageState.DEAD, broker.getMessageState("g-retry", 0));
        assertEquals(0L, broker.getCommittedOffset("g-retry"), "死信也是终态，位点应推进过该消息");
        // 退避期间 poll 不应提前投递（零退避时第一次 poll 即可拿到，重试次数仍然精确为 3）
        TraceEvent exhaust = broker.lastTraceFor("g-retry", "m-r");
        assertEquals(DecisionCode.NACK_RETRY_EXHAUSTED, exhaust.code());
    }

    @Test
    void nonRetryableFailureGoesDeadLetterImmediately() {
        LocalMessageBroker broker = brokerWithMaxAttempts("g-nonretry", 5);
        broker.enqueue(new Message("m-n", "orders", "bad-payload", 1));

        LeasedMessage lease = broker.poll("g-nonretry", "c1");
        assertNotNull(lease);
        broker.acknowledgeFailure(lease, false, "INVALID_MESSAGE_FORMAT");

        LeasedMessage second = broker.poll("g-nonretry", "c1");

        System.out.println("【输入】不可重试失败 offset=0 reason=INVALID_MESSAGE_FORMAT");
        System.out.println("【判定】尝试数=" + broker.getAttempts("g-nonretry", 0).size()
                + " 再poll结果=" + second);
        System.out.println("【位点】committed=" + broker.getCommittedOffset("g-nonretry"));
        System.out.println("【依据】" + broker.lastTraceFor("g-nonretry", "m-n").code()
                + " | " + broker.lastTraceFor("g-nonretry", "m-n").detail());

        assertNull(second, "不可重试消息不得再投递");
        assertEquals(1, broker.getAttempts("g-nonretry", 0).size());
        assertEquals("NON_RETRYABLE_BY_HANDLER", broker.getDeadLetters().get(0).cause());
        assertEquals(DecisionCode.NACK_NON_RETRYABLE,
                broker.lastTraceFor("g-nonretry", "m-n").code());
        assertEquals(0L, broker.getCommittedOffset("g-nonretry"));
    }
}
