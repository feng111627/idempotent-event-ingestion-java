package com.github.feng111627.idempotenteventingestionjava.domain;

/** 事件处理状态机：状态只允许单调推进，不允许回退。 */
public enum EventStatus {
    RECEIVED,
    PROCESSING,
    RETRYING,
    DELIVERED,
    DEAD_LETTER
}
