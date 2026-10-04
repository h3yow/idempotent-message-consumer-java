package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个消费者组的可持久化状态快照。
 *
 * <p>位点规则（核心）：{@code committedOffset} 表示“所有 offset &lt;= committedOffset 的消息
 * 都已到达终态（SUCCEEDED/DEAD）”，即已提交位点之前不存在未确认消息。正常消费位点只能单调递增；
 * 位点唯一的“回退”入口是显式运维操作 {@code rewindCommittedOffset}，回退区间内不得存在
 * 尚未确认（IN_FLIGHT）的消息。</p>
 *
 * <p>每条消息的状态、当前租约、全部尝试轨迹都在此保留；重试与重投递只更新同一条记录，
 * 因此不会丢失原始记录，也不会污染已成功的状态。死信重投与进度回退产生的每一次重放动作
 * 也逐笔记录在 {@link Entry#replays} 中，重启后审计链不断。</p>
 */
public final class GroupSnapshot {

    /** 单个消费者组内某条消息的可变处理记录。 */
    public static final class Entry {
        public long offset;
        public String messageId;
        public MessageState state = MessageState.PENDING;
        public int attemptCount;
        public String currentLeaseId;
        public String currentConsumerId;
        public long leaseExpiresAt;
        /** RETRY_WAIT 状态下，最早允许再次投递的时间（epoch millis）。 */
        public long nextVisibleAt;
        public List<AttemptRecord> attempts = new ArrayList<>();
        /** 该消息是否已经在本组成功生效过；重放只允许重放业务效果一次的审计依据。 */
        public boolean succeededOnce;
        /** 当前所处的重放轮次（0=非重放；>=1 表示第几次死信重投/进度回退重放），到达终态后清零。 */
        public int activeReplayNo;
        /** 当前重放轮次的来源（NORMAL/DEAD_LETTER_REPLAY/OFFSET_REWIND）。 */
        public DeliveryKind activeReplayKind = DeliveryKind.NORMAL;
        /** 当前重放轮次的操作人（可为 null）。 */
        public String activeReplayOperator;
        /** 每次运维重放动作（死信重投/进度回退作用于本条消息）的持久化审计记录。 */
        public List<ReplayRecord> replays = new ArrayList<>();

        public Entry() {
        }

        public Entry(long offset, String messageId) {
            this.offset = offset;
            this.messageId = messageId;
        }

        /** 兼容旧快照：反序列化后把可能缺失的字段归一化。 */
        @JsonIgnore
        public void normalizeAfterLoad() {
            if (attempts == null) {
                attempts = new ArrayList<>();
            }
            if (replays == null) {
                replays = new ArrayList<>();
            }
            if (activeReplayKind == null) {
                activeReplayKind = DeliveryKind.NORMAL;
            }
        }

        /** 该消息累计被运维重放了多少次（含当前进行中的轮次）。 */
        @JsonIgnore
        public int replayCount() {
            return replays == null ? 0 : replays.size();
        }
    }

    private String group;
    /** 已提交位点：语义见类注释；-1 表示尚无任何消息被提交。 */
    private long committedOffset = -1;
    private final List<Entry> entries = new ArrayList<>();

    public GroupSnapshot() {
    }

    public GroupSnapshot(String group) {
        this.group = group;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public long getCommittedOffset() {
        return committedOffset;
    }

    public void setCommittedOffset(long committedOffset) {
        this.committedOffset = committedOffset;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public Entry entryAt(long offset) {
        int idx = (int) offset;
        if (idx < 0 || idx >= entries.size()) {
            return null;
        }
        Entry e = entries.get(idx);
        return e.offset == offset ? e : null;
    }

    /** 加载旧版本快照后调用：把可能缺失的新字段全部归一化，避免 NPE。 */
    public void normalizeAfterLoad() {
        for (Entry e : entries) {
            e.normalizeAfterLoad();
        }
    }
}
