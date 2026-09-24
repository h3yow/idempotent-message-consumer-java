package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.github.h3yow.idempotentmessageconsumerjava.mq.handler.MessageHandler;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.ConsumeOutcome;
import com.github.h3yow.idempotentmessageconsumerjava.mq.model.LeasedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费者执行体：在独立线程中循环 poll → 调业务处理器 → ack/nack。
 *
 * <p>多个 worker 使用相同 group、不同 consumerId 并发运行，由 broker 的全局锁串行化状态变更；
 * 位点只由 broker 按“连续终态水位”推进，因此并发确认不会越过未确认消息。</p>
 *
 * <p>处理器抛出的任何异常都按可重试失败上报；显式返回 {@code dead(...)} 才直接死信。</p>
 */
public class ConsumerWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

    private final LocalMessageBroker broker;
    private final String group;
    private final String consumerId;
    private final MessageHandler handler;
    private final long idleSleepMillis;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private Thread thread;

    public ConsumerWorker(LocalMessageBroker broker, String group, String consumerId,
                          MessageHandler handler) {
        this(broker, group, consumerId, handler, 20);
    }

    public ConsumerWorker(LocalMessageBroker broker, String group, String consumerId,
                          MessageHandler handler, long idleSleepMillis) {
        this.broker = broker;
        this.group = group;
        this.consumerId = consumerId;
        this.handler = handler;
        this.idleSleepMillis = idleSleepMillis;
    }

    /** 以守护线程启动。 */
    public ConsumerWorker start() {
        if (!running.compareAndSet(false, true)) {
            return this;
        }
        thread = new Thread(this, "mq-worker-" + group + "-" + consumerId);
        thread.setDaemon(true);
        thread.start();
        return this;
    }

    /** 请求停止并等待循环退出。 */
    public void stop(long timeoutMillis) throws InterruptedException {
        running.set(false);
        stopped.await(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        if (thread != null) {
            thread.interrupt();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public long processedCount() {
        return processed.get();
    }

    public long failedCount() {
        return failed.get();
    }

    @Override
    public void run() {
        log.info("[MQ-WORKER] consumer={} group={} started", consumerId, group);
        try {
            while (running.get()) {
                LeasedMessage lease;
                try {
                    lease = broker.poll(group, consumerId);
                } catch (RuntimeException e) {
                    log.warn("[MQ-WORKER] poll failed consumer={}: {}", consumerId, e.toString());
                    sleep(idleSleepMillis);
                    continue;
                }
                if (lease == null) {
                    sleep(idleSleepMillis);
                    continue;
                }
                handleOne(lease);
            }
        } finally {
            log.info("[MQ-WORKER] consumer={} group={} stopping, processed={}, failed={}",
                    consumerId, group, processed.get(), failed.get());
            stopped.countDown();
        }
    }

    private void handleOne(LeasedMessage lease) {
        log.info("[MQ-WORKER] consumer={} received group={} offset={} messageId={} attemptNo={}",
                consumerId, group, lease.offset(), lease.message().id(), lease.attemptNo());
        try {
            ConsumeOutcome outcome = handler.handle(lease.message());
            if (outcome == null) {
                outcome = ConsumeOutcome.success();
            }
            broker.complete(lease, outcome, null);
            processed.incrementAndGet();
            if (outcome.verdict() != ConsumeOutcome.Verdict.SUCCESS) {
                failed.incrementAndGet();
            }
        } catch (Exception e) {
            failed.incrementAndGet();
            log.warn("[MQ-WORKER] handler threw consumer={} offset={} messageId={}: {}",
                    consumerId, lease.offset(), lease.message().id(), e.toString());
            // 处理器异常一律按可重试失败处理，原因记录为异常类型
            broker.acknowledgeFailure(lease, true,
                    "HANDLER_EXCEPTION:" + e.getClass().getSimpleName());
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running.set(false);
        }
    }
}
