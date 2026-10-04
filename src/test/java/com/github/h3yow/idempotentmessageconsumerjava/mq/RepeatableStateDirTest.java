package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.DecisionCode;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.EnqueueResult;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景九：同一状态目录重复执行同一套操作，结果必须一致（可反复回归）。
 * 模拟“同一个状态目录连着跑两遍”：第二遍在同一快照文件上重启 broker、
 * 重放完全相同的操作脚本，断言第二遍不产生任何新的业务效果、
 * 位点/状态/死信/尝试次数与第一遍结束时完全一致。
 */
class RepeatableStateDirTest {

    @TempDir
    Path tmp;

    private final BackoffPolicy noBackoff = failedAttemptNo -> 0L;

    private LocalMessageBroker startBroker() {
        LocalMessageBroker broker = new LocalMessageBroker(
                new FileBrokerStore(tmp.resolve("state.json")));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", 1,
                Duration.ofSeconds(30), noBackoff));
        return broker;
    }

    /** 相同输入跑两遍的固定脚本：入队 3 条；m-1 先失败进死信再人工重投成功；其余成功。 */
    private Map<String, String> runScript(LocalMessageBroker broker, String runTag) {
        Map<String, String> outcome = new LinkedHashMap<>();
        for (int i = 0; i < 3; i++) {
            EnqueueResult r = broker.enqueue(
                    new Message("m-" + i, "orders", "p" + i, 1000 + i));
            outcome.put("enqueue.m-" + i, r.code().name());
        }
        int deliveries = 0;
        LeasedMessage lease;
        while ((lease = broker.poll("g", "c1")) != null) {
            deliveries++;
            if (lease.message().id().equals("m-1") && lease.attemptNo() == 1) {
                broker.acknowledgeFailure(lease, false, "BAD_ONCE");
                outcome.put("m-1.first", "DEAD");
                // 人工重投死信
                broker.redeliverDeadLetter("g", "m-1");
                outcome.put("m-1.redeliver", "OK");
            } else {
                broker.acknowledgeSuccess(lease);
            }
        }
        outcome.put("deliveries", String.valueOf(deliveries));
        outcome.put("committed", String.valueOf(broker.getCommittedOffset("g")));
        for (int i = 0; i < 3; i++) {
            outcome.put("state.m-" + i, String.valueOf(broker.getMessageState("g", i)));
            outcome.put("attempts.m-" + i, String.valueOf(broker.getAttemptCount("g", i)));
        }
        outcome.put("deadLetters", String.valueOf(broker.getDeadLetters().size()));
        System.out.println("【" + runTag + "】脚本执行结果: " + new TreeMap<>(outcome));
        return outcome;
    }

    @Test
    void sameScriptOnSameStateDirTwiceYieldsConsistentResults() {
        // 第一遍
        LocalMessageBroker first = startBroker();
        Map<String, String> run1 = runScript(first, "第一遍");
        assertEquals("2", run1.get("committed"));
        assertEquals("SUCCEEDED", run1.get("state.m-1"));
        assertEquals("1", run1.get("deadLetters"));
        assertEquals("4", run1.get("deliveries"), "3 条消息 + 1 次死信重投");

        // 第二遍：同一状态目录重启，重放同一脚本
        LocalMessageBroker second = startBroker();
        Map<String, String> run2 = runScript(second, "第二遍");

        System.out.println("【判定】第二遍入队结论应全部为重复忽略: "
                + run2.get("enqueue.m-0") + "," + run2.get("enqueue.m-1") + ","
                + run2.get("enqueue.m-2"));
        System.out.println("【位点】第一遍末 committed=" + run1.get("committed")
                + " 第二遍末 committed=" + run2.get("committed"));

        // 第二遍：所有入队被识别为重复；没有任何新投递；终态与第一遍完全一致
        assertEquals(DecisionCode.ENQUEUE_DUPLICATED_GLOBAL_ID.name(), run2.get("enqueue.m-0"));
        assertEquals(DecisionCode.ENQUEUE_DUPLICATED_GLOBAL_ID.name(), run2.get("enqueue.m-1"));
        assertEquals(DecisionCode.ENQUEUE_DUPLICATED_GLOBAL_ID.name(), run2.get("enqueue.m-2"));
        assertEquals("0", run2.get("deliveries"), "第二遍不得有任何新投递/新生效");
        for (String key : new String[]{"committed", "state.m-0", "state.m-1", "state.m-2",
                "attempts.m-0", "attempts.m-1", "attempts.m-2", "deadLetters"}) {
            assertEquals(run1.get(key), run2.get(key),
                    "两遍结果必须一致: " + key);
        }
        // 死信重投过的消息在第二遍仍是成功终态，不会因脚本重放而再次失败或再次投递
        assertEquals(MessageState.SUCCEEDED, second.getMessageState("g", 1));
        assertEquals(1, second.getDeadLetters().size());
    }
}
