package com.github.h3yow.idempotentmessageconsumerjava.mq.broker;

import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.BackoffPolicy;
import com.github.h3yow.idempotentmessageconsumerjava.mq.retry.ExponentialBackoff;

import java.time.Duration;

/**
 * 消费者组配置：重试上限、退避策略、租约时长，以及该组订阅的主题。
 *
 * @param group          消费者组名（不同组各自维护位点与生效状态，广播式独立消费）
 * @param topic          订阅主题
 * @param maxAttempts    最大尝试次数（含首次）；超过后进入死信
 * @param leaseDuration  租约时长，超时未确认的消息会被回收重投
 * @param backoffPolicy  退避策略
 */
public record ConsumerGroupConfig(String group,
                                  String topic,
                                  int maxAttempts,
                                  Duration leaseDuration,
                                  BackoffPolicy backoffPolicy) {

    public ConsumerGroupConfig {
        if (group == null || group.isBlank()) {
            throw new IllegalArgumentException("group must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        if (backoffPolicy == null) {
            backoffPolicy = new ExponentialBackoff(50, 2.0, 5_000, 25);
        }
    }

    /** 常用默认：3 次尝试、10 秒租约、指数退避。 */
    public static ConsumerGroupConfig of(String group, String topic) {
        return new ConsumerGroupConfig(group, topic, 3, Duration.ofSeconds(10),
                new ExponentialBackoff(50, 2.0, 5_000, 25));
    }
}
