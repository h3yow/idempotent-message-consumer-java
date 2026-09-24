package com.github.h3yow.idempotentmessageconsumerjava.mq.model;

/**
 * 单次消费尝试（attempt）的结果分类。
 *
 * <ul>
 *   <li>{@link #SUCCESS}：处理器正常返回，业务生效；</li>
 *   <li>{@link #RETRYABLE_FAILURE}：处理器报告可重试失败（抛异常或显式重试结论），按退避策略重试；</li>
 *   <li>{@link #NON_RETRYABLE_FAILURE}：处理器报告不可重试失败，直接进入死信；</li>
 *   <li>{@link #LEASE_TIMEOUT}：租约超时未确认，由系统重新投递；</li>
 *   <li>{@link #DUPLICATE_SKIPPED}：本次租约对应的消息已为成功终态，幂等检查直接跳过（不执行业务）。</li>
 * </ul>
 */
public enum AttemptResult {
    SUCCESS,
    RETRYABLE_FAILURE,
    NON_RETRYABLE_FAILURE,
    LEASE_TIMEOUT,
    DUPLICATE_SKIPPED
}
