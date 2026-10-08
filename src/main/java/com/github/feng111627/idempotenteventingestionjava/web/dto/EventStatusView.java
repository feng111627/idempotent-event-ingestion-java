package com.github.feng111627.idempotenteventingestionjava.web.dto;

/**
 * 事件状态的一致查询视图，成功/重试中/失败/重复共用同一结构。
 */
public record EventStatusView(
        String eventId,
        String status,
        boolean inFailureZone,
        int attempts,
        int maxAttempts,
        int duplicateCount,
        String failureReason,
        String failureMessage,
        Long nextRetryAt,
        long receivedAt,
        long updatedAt,
        Long completedAt) {
}
