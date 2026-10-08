package com.github.feng111627.idempotenteventingestionjava.web;

/**
 * 对外可控异常：只携带 HTTP 状态、机器可读错误码与可读信息，不携带底层异常。
 */
public class ApiException extends RuntimeException {

    private final int httpStatus;
    private final String code;

    public ApiException(int httpStatus, String code, String message) {
        super(message);
        this.httpStatus = httpStatus;
        this.code = code;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getCode() {
        return code;
    }
}
