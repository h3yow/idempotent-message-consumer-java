package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/** 位点提交被拒绝的原因。 */
public enum RejectReason {
    REGRESSED,
    GAP_WITH_UNACKED,
    UNKNOWN_MESSAGE
}
