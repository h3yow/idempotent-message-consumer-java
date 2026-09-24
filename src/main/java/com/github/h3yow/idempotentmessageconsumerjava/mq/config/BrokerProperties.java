package com.github.h3yow.idempotentmessageconsumerjava.mq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地消息组件配置项（前缀 {@code localmq}）。
 *
 * <ul>
 *   <li>{@code localmq.enabled}：是否启用自动装配的 broker 单例，默认 true；</li>
 *   <li>{@code localmq.state-file}：JSON 快照文件路径，默认 {@code ./data/localmq-state.json}；</li>
 *   <li>{@code localmq.default-max-attempts}：未显式指定时的最大尝试次数，默认 3；</li>
 *   <li>{@code localmq.default-lease-seconds}：默认租约时长（秒），默认 10；</li>
 *   <li>{@code localmq.backoff-base-millis} / {@code multiplier} / {@code max-millis} /
 *       {@code jitter-millis}：指数退避参数。</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "localmq")
public class BrokerProperties {

    private boolean enabled = true;
    private String stateFile = "./data/localmq-state.json";
    private int defaultMaxAttempts = 3;
    private long defaultLeaseSeconds = 10;
    private long backoffBaseMillis = 50;
    private double backoffMultiplier = 2.0;
    private long backoffMaxMillis = 5_000;
    private long backoffJitterMillis = 25;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getStateFile() {
        return stateFile;
    }

    public void setStateFile(String stateFile) {
        this.stateFile = stateFile;
    }

    public int getDefaultMaxAttempts() {
        return defaultMaxAttempts;
    }

    public void setDefaultMaxAttempts(int defaultMaxAttempts) {
        this.defaultMaxAttempts = defaultMaxAttempts;
    }

    public long getDefaultLeaseSeconds() {
        return defaultLeaseSeconds;
    }

    public void setDefaultLeaseSeconds(long defaultLeaseSeconds) {
        this.defaultLeaseSeconds = defaultLeaseSeconds;
    }

    public long getBackoffBaseMillis() {
        return backoffBaseMillis;
    }

    public void setBackoffBaseMillis(long backoffBaseMillis) {
        this.backoffBaseMillis = backoffBaseMillis;
    }

    public double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public long getBackoffMaxMillis() {
        return backoffMaxMillis;
    }

    public void setBackoffMaxMillis(long backoffMaxMillis) {
        this.backoffMaxMillis = backoffMaxMillis;
    }

    public long getBackoffJitterMillis() {
        return backoffJitterMillis;
    }

    public void setBackoffJitterMillis(long backoffJitterMillis) {
        this.backoffJitterMillis = backoffJitterMillis;
    }
}
