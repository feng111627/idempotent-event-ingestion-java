package com.github.feng111627.idempotenteventingestionjava.delivery;

/** 下游落地端抽象。 */
public interface DownstreamClient {
    void deliver(String eventId, String payload);
}
