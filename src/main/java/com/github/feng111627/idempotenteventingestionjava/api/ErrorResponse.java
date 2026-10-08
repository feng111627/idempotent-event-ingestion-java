package com.github.feng111627.idempotenteventingestionjava.api;

/** 对外统一错误体，不透出底层异常。 */
public record ErrorResponse(String code, String message) {
}
