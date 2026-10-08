package com.github.feng111627.idempotenteventingestionjava.domain;

/**
 * 一条事件的完整处理记录（聚合根）。
 *
 * <p>字段对 Jackson 友好，可直接序列化为 JSON 落盘；所有变更必须经过
 * {@code EventStore} 加锁后写回，保证进程内并发与重启后的一致性。</p>
 */
public class EventRecord {

    /** 业务幂等键，由接入端在事件体内提供，全局唯一。 */
    private String eventId;

    /** 原始事件负载（JSON 文本原样保存），便于失败区排查。 */
    private String payload;

    /** 当前处理状态。 */
    private EventStatus status;

    /** 失败原因分类；仅 {@link EventStatus#FAILED} 时有值。 */
    private FailureReason failureReason;

    /** 面向调用方的可读失败说明，不含底层堆栈/异常类名。 */
    private String failureMessage;

    /** 已实际调用下游的次数（不含因并发去重而被短路的请求）。 */
    private int attempts;

    /** 最大尝试次数（含首次），超过后进入失败区。 */
    private int maxAttempts;

    /** 下一次允许重试的时间（epoch millis）；不在重试等待中时为 null。 */
    private Long nextRetryAt;

    /** 接收时间（epoch millis）。 */
    private long receivedAt;

    /** 最近一次状态更新时间（epoch millis）。 */
    private long updatedAt;

    /** 进入终态（SUCCESS/FAILED）的时间（epoch millis）。 */
    private Long completedAt;

    /** 被识别为重复投递的次数，用于校验并发下去重计数准确。 */
    private int duplicateCount;

    public EventRecord() {
    }

    public EventRecord(String eventId, String payload, int maxAttempts, long now) {
        this.eventId = eventId;
        this.payload = payload;
        this.status = EventStatus.RECEIVED;
        this.maxAttempts = maxAttempts;
        this.receivedAt = now;
        this.updatedAt = now;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public FailureReason getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(FailureReason failureReason) {
        this.failureReason = failureReason;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Long getNextRetryAt() {
        return nextRetryAt;
    }

    public void setNextRetryAt(Long nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public long getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(long receivedAt) {
        this.receivedAt = receivedAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Long completedAt) {
        this.completedAt = completedAt;
    }

    public int getDuplicateCount() {
        return duplicateCount;
    }

    public void setDuplicateCount(int duplicateCount) {
        this.duplicateCount = duplicateCount;
    }
}
