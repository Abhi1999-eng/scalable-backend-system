package com.abhishek.scalable_backend_system.model;

public class UserEvent {

    private String eventType;
    private Long userId;
    private long timestamp;

    public UserEvent() {}

    public UserEvent(String eventType, Long userId, long timestamp) {
        this.eventType = eventType;
        this.userId = userId;
        this.timestamp = timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public Long getUserId() {
        return userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "eventType='" + eventType + '\'' +
                ", userId=" + userId +
                ", timestamp=" + timestamp +
                '}';
    }
}