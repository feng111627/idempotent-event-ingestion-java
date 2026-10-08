package com.github.feng111627.idempotenteventingestionjava.downstream;

import com.github.feng111627.idempotenteventingestionjava.domain.EventRecord;

/**
 * 本地模拟的下游落地端。实现需自带落地去重能力（防御性幂等）。
 */
public interface DownstreamClient {

    /**
     * 将事件投递到落地端。
     *
     * @throws DownstreamException 校验失败（不可重试）或下游不可用（可重试）
     */
    DeliveryResult deliver(EventRecord record);
}
