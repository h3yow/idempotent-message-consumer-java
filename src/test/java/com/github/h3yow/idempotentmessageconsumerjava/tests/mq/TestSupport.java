package com.github.h3yow.idempotentmessageconsumerjava.tests.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.IdempotentBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Delivery;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.OffsetCommitResult;

import java.util.List;

/** 测试中统一打印“输入 / 位点 / 判定依据”的辅助工具。 */
final class TestSupport {

    private TestSupport() {
    }

    static void title(String name) {
        System.out.println();
        System.out.println("==================== " + name + " ====================");
    }

    static void logInput(String group, String topic, String action, Object input) {
        System.out.println("[INPUT ] group=" + group + ", topic=" + topic
                + ", action=" + action + ", payload=" + input);
    }

    static void logDeliveries(String tag, List<Delivery> ds) {
        System.out.println("[DELIVER " + tag + "] count=" + ds.size());
        ds.forEach(d -> System.out.println("    -> " + d));
    }

    static void logOffset(String group, String topic, OffsetCommitResult r) {
        System.out.println("[OFFSET] group=" + group + ", topic=" + topic + " => " + r);
    }

    static void logRecord(IdempotentBroker broker, String group, String topic) {
        for (MessageRecord r : broker.records(group, topic)) {
            System.out.println("[RECORD] " + r);
            r.events().forEach(e -> System.out.println("           event@" + e.at() + " : " + e.detail()));
            r.attempts().forEach(a -> System.out.println("           attempt : " + a));
            if (r.isTerminal()) {
                System.out.println("           terminalReason=" + r.terminalReason()
                        + ", businessEffectCount=" + r.businessEffectCount());
            }
        }
    }
}
