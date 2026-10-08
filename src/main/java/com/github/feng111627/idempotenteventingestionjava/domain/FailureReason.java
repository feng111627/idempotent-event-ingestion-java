package com.github.feng111627.idempotenteventingestionjava.domain;

/** 可区分的失败原因。 */
public enum FailureReason {
    VALIDATION_FAILED,
    DOWNSTREAM_UNAVAILABLE,
    DUPLICATE_DELIVERY
}
