package com.github.feng111627.idempotenteventingestionjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 本地运行参数。全部可通过 application.properties / 环境变量覆盖，
 * 默认值保证零外部依赖即可运行与测试。
 *
 * @param dataDir           事件存储目录（本地文件）
 * @param downstreamDataDir 模拟下游落地区目录（本地文件）
 * @param maxAttempts       最大投递尝试次数（含首次）
 * @param backoffBaseMillis 指数退避基数：第 n 次重试等待 base * 2^(n-1)
 * @param backoffMaxMillis  单次退避上限
 * @param retryJitter       是否启用随机抖动（测试中可关闭以保证确定性）
 * @param scanIntervalMillis 重试扫描周期
 */
@ConfigurationProperties(prefix = "app.ingestion")
public record AppProperties(
        String dataDir,
        String downstreamDataDir,
        int maxAttempts,
        long backoffBaseMillis,
        long backoffMaxMillis,
        boolean retryJitter,
        long scanIntervalMillis) {

    public AppProperties {
        dataDir = (dataDir == null || dataDir.isBlank()) ? "data/ingestion" : dataDir;
        downstreamDataDir = (downstreamDataDir == null || downstreamDataDir.isBlank())
                ? "data/downstream" : downstreamDataDir;
        maxAttempts = maxAttempts <= 0 ? 3 : maxAttempts;
        backoffBaseMillis = backoffBaseMillis <= 0 ? 200 : backoffBaseMillis;
        backoffMaxMillis = backoffMaxMillis <= 0 ? 5_000 : backoffMaxMillis;
        scanIntervalMillis = scanIntervalMillis <= 0 ? 500 : scanIntervalMillis;
    }
}
