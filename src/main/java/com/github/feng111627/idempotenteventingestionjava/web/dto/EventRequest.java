package com.github.feng111627.idempotenteventingestionjava.web.dto;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 接入端投递事件的请求体。
 *
 * @param eventId 业务幂等键，必填；相同 eventId 的并发/重复投递只会落地一次
 * @param payload 事件内容，必须为 JSON 对象；本地校验规则由下游落地端定义
 */
public record EventRequest(String eventId, JsonNode payload) {
}
