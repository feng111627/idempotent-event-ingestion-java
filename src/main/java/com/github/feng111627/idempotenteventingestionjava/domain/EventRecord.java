package com.github.feng111627.idempotenteventingestionjava.domain;

import java.time.Instant;

/** 事件处理记录，承载一致的状态视图。 */
public class EventRecord {
    private String eventId;
    private String payload;
    private EventStatus status;
    private int attempts;
    private int duplicateAttempts;
    private FailureReason failureReason;
    private String failureDetail;
    private Instant createdAt;
    private Instant updatedAt;

    public EventRecord() {
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public EventStatus getStatus() { return status; }
    public void setStatus(EventStatus status) { this.status = status; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public int getDuplicateAttempts() { return duplicateAttempts; }
    public void setDuplicateAttempts(int duplicateAttempts) { this.duplicateAttempts = duplicateAttempts; }
    public FailureReason getFailureReason() { return failureReason; }
    public void setFailureReason(FailureReason failureReason) { this.failureReason = failureReason; }
    public String getFailureDetail() { return failureDetail; }
    public void setFailureDetail(String failureDetail) { this.failureDetail = failureDetail; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
