package com.github.feng111627.idempotenteventingestionjava.web.dto;

/**
 * 投递结果。首次投递与重复投递通过 {@code duplicate} 与 {@code httpStatusHint} 区分，
 * 不会静默丢弃，也不会重复写入。
 *
 * @param duplicate      是否为重复投递（true 表示此前已接收过同一 eventId）
 * @param httpStatusHint 建议的 HTTP 状态码：首次 202，重复 200
 * @param status         当前处理状态（RECEIVED/PROCESSING/RETRYING/SUCCESS/FAILED）
 * @param attempts       已实际调用下游次数
 * @param failureReason  失败原因分类（SUCCESS/非失败时为 null）
 * @param failureMessage 可读失败说明（不含底层异常）
 * @param nextRetryAt    下一次重试时间 epoch millis（无则 null）
 * @param completedAt    终态完成时间 epoch millis（无则 null）
 */
public record EventResponse(
        String eventId,
        boolean duplicate,
        int httpStatusHint,
        String status,
        int attempts,
        String failureReason,
        String failureMessage,
        Long nextRetryAt,
        Long completedAt) {
}
