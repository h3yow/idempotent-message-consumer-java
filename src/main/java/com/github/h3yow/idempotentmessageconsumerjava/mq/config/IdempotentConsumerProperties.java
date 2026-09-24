package com.github.h3yow.idempotentmessageconsumerjava.mq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * 幂等消息消费组件配置项，前缀 {@code idempotent-mq}。
 *
 * <pre>
 * idempotent-mq.max-attempts=3
 * idempotent-mq.backoff-initial=100ms
 * idempotent-mq.backoff-multiplier=2.0
 * idempotent-mq.backoff-max=10s
 * idempotent-mq.visibility-lease=30s
 * idempotent-mq.state-file=build/mq/broker-state.json   # 不配置则使用纯内存存储
 * </pre>
 */
@ConfigurationProperties(prefix = "idempotent-mq")
public class IdempotentConsumerProperties {

    /** 一条消息允许的总投递次数（含首次）；达到上限仍失败则进死信。 */
    private int maxAttempts = 3;

    /** 首次失败后的退避时长。 */
    private Duration backoffInitial = Duration.ofMillis(100);

    /** 退避倍数。 */
    private double backoffMultiplier = 2.0;

    /** 退避上限。 */
    private Duration backoffMax = Duration.ofSeconds(10);

    /** 投递租约：超过该时长仍未确认的消息可被重新投递。 */
    private Duration visibilityLease = Duration.ofSeconds(30);

    /**
     * 状态文件路径。配置后使用本地 JSON 持久化（支持重启恢复）；
     * 留空则使用纯内存存储（进程结束即丢失）。
     */
    private String stateFile;

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getBackoffInitial() {
        return backoffInitial;
    }

    public void setBackoffInitial(Duration backoffInitial) {
        this.backoffInitial = backoffInitial;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public Duration getBackoffMax() {
        return backoffMax;
    }

    public void setBackoffMax(Duration backoffMax) {
        this.backoffMax = backoffMax;
    }

    public Duration getVisibilityLease() {
        return visibilityLease;
    }

    public void setVisibilityLease(Duration visibilityLease) {
        this.visibilityLease = visibilityLease;
    }

    public String getStateFile() {
        return stateFile;
    }

    public void setStateFile(String stateFile) {
        this.stateFile = stateFile;
    }
}
