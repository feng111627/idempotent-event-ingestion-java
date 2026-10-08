package com.github.feng111627.idempotenteventingestionjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 重试与退避策略配置。 */
@Component
@ConfigurationProperties(prefix = "ingestion.retry")
public class RetryProperties {
    private int maxAttempts = 3;
    private long initialBackoffMs = 100;
    private double backoffMultiplier = 2.0;

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getInitialBackoffMs() { return initialBackoffMs; }
    public void setInitialBackoffMs(long initialBackoffMs) { this.initialBackoffMs = initialBackoffMs; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
}
