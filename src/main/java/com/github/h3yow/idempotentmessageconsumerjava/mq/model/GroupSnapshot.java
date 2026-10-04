package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个消费者组的可持久化状态快照。
 *
 * <p>位点规则（核心）：{@code committedOffset} 表示“所有 offset &lt;= committedOffset 的消息
 * 都已到达终态（SUCCEEDED/DEAD）”，即已提交位点之前不存在未确认消息。位点只能单调递增；
 * 回退或跨过未确认消息的提交会被拒绝。</p>
 *
 * <p>每条消息的状态、当前租约、全部尝试轨迹都在此保留；重试与重投递只更新同一条记录，
 * 因此不会丢失原始记录，也不会污染已成功的状态。</p>
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
        /** 人工死信重投次数。每次重投在重试上限之外恰好多给一次投递机会。 */
        public int redeliveryCount;
        /** 因进度重放被重置回待投递的次数。 */
        public int replayCount;
        /** 非 null 表示本次回到 PENDING 的原因："REDELIVER"（人工重投）/ "REPLAY"（进度重放），
         *  投递时写入轨迹后清除；持久化字段，重启后不丢失。 */
        public String pendingCause;
        public List<AttemptRecord> attempts = new ArrayList<>();

        public Entry() {
        }

        public Entry(long offset, String messageId) {
            this.offset = offset;
            this.messageId = messageId;
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
}
