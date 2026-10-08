package com.github.feng111627.idempotenteventingestionjava.delivery;

/** 下游临时不可用（可重试）。 */
public class DownstreamUnavailableException extends RuntimeException {
    public DownstreamUnavailableException(String message) {
        super(message);
    }
}
