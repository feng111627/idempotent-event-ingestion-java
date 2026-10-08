package com.github.feng111627.idempotenteventingestionjava.domain;

/** 接入响应分类，调用方可据此区分新事件与重复投递。 */
public enum IngestResult {
    ACCEPTED,
    DUPLICATE,
    REJECTED
}
