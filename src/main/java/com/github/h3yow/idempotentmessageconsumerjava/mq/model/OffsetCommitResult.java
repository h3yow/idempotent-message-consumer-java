package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 位点提交结果。
 *
 * <p>提交规则：只能提交连续已确认（SUCCEEDED 或 DEAD）前缀的下一位点。
 * 回退提交、越过未确认消息的乱序提交都会被明确拒绝，并给出可区分原因。
 */
public final class OffsetCommitResult {

    private final boolean accepted;
    private final long requestedOffset;
    private final long committedOffsetBefore;
    private final long committedOffsetAfter;
    private final RejectReason rejectReason;
    private final String detail;

    private OffsetCommitResult(boolean accepted, long requestedOffset,
                               long committedOffsetBefore, long committedOffsetAfter,
                               RejectReason rejectReason, String detail) {
        this.accepted = accepted;
        this.requestedOffset = requestedOffset;
        this.committedOffsetBefore = committedOffsetBefore;
        this.committedOffsetAfter = committedOffsetAfter;
        this.rejectReason = rejectReason;
        this.detail = detail == null ? "" : detail;
    }

    public static OffsetCommitResult accepted(long requested, long before, long after) {
        return new OffsetCommitResult(true, requested, before, after, null,
                "offset advanced " + before + " -> " + after);
    }

    public static OffsetCommitResult noop(long current) {
        return new OffsetCommitResult(true, current, current, current, null,
                "offset already at " + current + ", nothing to advance");
    }

    public static OffsetCommitResult rejected(long requested, long current,
                                              RejectReason reason, String detail) {
        return new OffsetCommitResult(false, requested, current, current, reason, detail);
    }

    public boolean accepted() { return accepted; }
    public long requestedOffset() { return requestedOffset; }
    public long committedOffsetBefore() { return committedOffsetBefore; }
    public long committedOffsetAfter() { return committedOffsetAfter; }
    public RejectReason rejectReason() { return rejectReason; }
    public String detail() { return detail; }

    @Override
    public String toString() {
        return accepted
                ? "OffsetCommitResult[ACCEPTED requested=" + requestedOffset
                + ", " + committedOffsetBefore + "->" + committedOffsetAfter + "]"
                : "OffsetCommitResult[REJECTED requested=" + requestedOffset
                + ", current=" + committedOffsetBefore + ", reason=" + rejectReason
                + ", " + detail + "]";
    }
}
