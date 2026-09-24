package com.github.h3yow.idempotentmessageconsumerjava.mq.handler;

import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.Message;

/**
 * 业务消息处理器。
 *
 * <p>约定：</p>
 * <ul>
 *   <li>正常处理完成返回 {@link ConsumeOutcome#success()}；</li>
 *   <li>可重试失败可返回 {@code ConsumeOutcome.retry(reason)}，或直接抛出异常
 *       （抛异常一律按可重试失败处理，原因记录为异常类型）；</li>
 *   <li>不可重试失败返回 {@link ConsumeOutcome#dead(String)}，消息直接进入死信。</li>
 * </ul>
 *
 * <p>中间件保证：同一消费者组内同一条消息的成功结论只会生效一次；即便处理器被重复执行，
 * 已 SUCCEEDED 的消息不会再被投递到处理器（幂等由中间件与业务去重表共同保证）。</p>
 */
@FunctionalInterface
public interface MessageHandler {

    ConsumeOutcome handle(Message message) throws Exception;
}
