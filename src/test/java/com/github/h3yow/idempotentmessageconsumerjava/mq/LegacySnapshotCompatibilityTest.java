package com.github.h3yow.idempotentmessageconsumerjava.mq;

import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.ConsumerGroupConfig;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.FileBrokerStore;
import com.github.h3yow.idempotentmessageconsumerjava.mq.broker.LocalMessageBroker;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.AttemptRecord;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeadLetter;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.DeliveryKind;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.MessageState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 旧版本快照（不含 succeededOnce / activeReplay* / replays / AttemptRecord.deliveryKind /
 * DeadLetter.replayNo 等新字段）加载后必须自动归一化，不抛异常且行为正确：
 * IN_FLIGHT 恢复重投、死信记录可识别为 replayNo=0、新字段默认值语义正确。
 */
class LegacySnapshotCompatibilityTest {

    @TempDir
    Path tmp;

    @Test
    void legacySnapshotWithoutNewFieldsLoadsAndRecoversNormally() throws Exception {
        Path file = tmp.resolve("legacy.json");
        // 模拟旧版本写出的快照：offset=0 IN_FLIGHT（attemptCount=1），offset=1 已 DEAD
        String json = """
                {
                  "messages" : {
                    "m-old-1" : { "id" : "m-old-1", "topic" : "orders", "payload" : "a", "enqueueAt" : 1 },
                    "m-old-2" : { "id" : "m-old-2", "topic" : "orders", "payload" : "b", "enqueueAt" : 2 }
                  },
                  "groups" : {
                    "g" : {
                      "group" : "g",
                      "committedOffset" : -1,
                      "entries" : [ {
                        "offset" : 0,
                        "messageId" : "m-old-1",
                        "state" : "IN_FLIGHT",
                        "attemptCount" : 1,
                        "currentLeaseId" : "old-lease-0",
                        "currentConsumerId" : "c-old",
                        "leaseExpiresAt" : 100,
                        "nextVisibleAt" : 0,
                        "attempts" : [ {
                          "attemptNo" : 1,
                          "consumerId" : "c-old",
                          "startedAt" : 10,
                          "finishedAt" : 100,
                          "result" : "RETRYABLE_FAILURE",
                          "reason" : "HANDLER_EXCEPTION:RuntimeException",
                          "detail" : "boom"
                        } ]
                      }, {
                        "offset" : 1,
                        "messageId" : "m-old-2",
                        "state" : "DEAD",
                        "attemptCount" : 1,
                        "currentLeaseId" : null,
                        "currentConsumerId" : null,
                        "leaseExpiresAt" : 0,
                        "nextVisibleAt" : 0,
                        "attempts" : [ {
                          "attemptNo" : 1,
                          "consumerId" : "c-old",
                          "startedAt" : 5,
                          "finishedAt" : 9,
                          "result" : "NON_RETRYABLE_FAILURE",
                          "reason" : "BAD",
                          "detail" : null
                        } ]
                      } ]
                    }
                  },
                  "deadLetters" : [ {
                    "message" : { "id" : "m-old-2", "topic" : "orders", "payload" : "b", "enqueueAt" : 2 },
                    "group" : "g",
                    "attempts" : [ ],
                    "deadAt" : 9,
                    "cause" : "NON_RETRYABLE_BY_HANDLER"
                  } ]
                }
                """;
        Files.writeString(file, json);

        LocalMessageBroker broker = new LocalMessageBroker(new FileBrokerStore(file));
        broker.registerGroup(new ConsumerGroupConfig("g", "orders", 3,
                Duration.ofHours(1), failed -> 0L));

        System.out.println("【输入】旧格式快照：无任何 replay/succeededOnce/deliveryKind 字段");
        System.out.println("【位点】加载恢复后 committed=" + broker.getCommittedOffset("g"));

        // 旧的 IN_FLIGHT 被恢复重投，attemptNo 延续为 2；旧尝试记录的 deliveryKind 归一化为 NORMAL
        LeasedMessage lease = broker.poll("g", "c-new");
        assertNotNull(lease);
        assertEquals(0L, lease.offset());
        assertEquals(2, lease.attemptNo());
        assertEquals(DeliveryKind.NORMAL, lease.deliveryKind());
        assertFalse(broker.hasBusinessEffectTaken("g", 0), "旧快照没有成功历史，护栏应为 false");
        broker.acknowledgeSuccess(lease);
        assertTrue(broker.hasBusinessEffectTaken("g", 0));

        // 旧死信记录缺省 replayNo 归一化为 0，且仍可对它发起人工重投
        List<DeadLetter> deadLetters = broker.getDeadLetters();
        assertEquals(1, deadLetters.size());
        assertEquals(0, deadLetters.get(0).replayNo(), "旧死信记录必须被识别为 replayNo=0");
        broker.replayDeadLetter("g", 1, "ops", "legacy compensation");
        LeasedMessage replay = broker.poll("g", "c-new");
        assertNotNull(replay);
        assertEquals(DeliveryKind.DEAD_LETTER_REPLAY, replay.deliveryKind());
        assertEquals(1, replay.replayNo());

        // 归一化后再次落盘、再次加载也不报错（前向/后向往返）
        broker.flush();
        LocalMessageBroker reloaded = new LocalMessageBroker(new FileBrokerStore(file));
        reloaded.registerGroup(new ConsumerGroupConfig("g", "orders", 3,
                Duration.ofHours(1), failed -> 0L));
        assertEquals(MessageState.PENDING, reloaded.getMessageState("g", 1),
                "重放租约在再加载时按未确认恢复为 PENDING");
        assertEquals(1, reloaded.getReplays("g", 1).size());
        AttemptRecord oldAttempt = reloaded.getAttempts("g", 0).get(0);
        assertEquals(DeliveryKind.NORMAL, oldAttempt.deliveryKind(),
                "旧尝试记录反序列化后 deliveryKind=NORMAL");
        System.out.println("【判定】旧快照加载、恢复、重投、再落盘、再加载全部正常");
    }
}
