package com.github.feng111627.idempotenteventingestionjava.api;

import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.FailureReason;

import java.time.Instant;

/** 对外的一致状态视图。 */
public record EventStatusView(
        String eventId,
        EventStatus status,
        int attempts,
        int duplicateAttempts,
        FailureReason failureReason,
        String failureDetail,
        Instant createdAt,
        Instant updatedAt) {
}
