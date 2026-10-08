package com.github.feng111627.idempotenteventingestionjava.ingest;

/** 校验失败异常（仅内部使用，不直接透出到接口）。 */
public class EventValidationException extends RuntimeException {
    public EventValidationException(String message) {
        super(message);
    }
}
