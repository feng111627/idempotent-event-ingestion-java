package com.github.feng111627.idempotenteventingestionjava.api;

import com.github.feng111627.idempotenteventingestionjava.domain.EventStatus;
import com.github.feng111627.idempotenteventingestionjava.domain.IngestResult;

/** 接入响应，重复投递可区分。 */
public record IngestResponse(String eventId, IngestResult result, EventStatus status, String message) {
}
