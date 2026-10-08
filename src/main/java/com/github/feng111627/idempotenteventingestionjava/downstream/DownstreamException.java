package com.github.feng111627.idempotenteventingestionjava.downstream;

import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;

/**
 * 下游落地端抛出的业务异常。只在进程内部流转，绝不直接透出到对外接口。
 */
public class DownstreamException extends RuntimeException {

    private final boolean retryable;
    private final FailureReason reason;

    public DownstreamException(FailureReason reason, boolean retryable, String message) {
        super(message);
        this.reason = reason;
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public FailureReason getReason() {
        return reason;
    }
}
