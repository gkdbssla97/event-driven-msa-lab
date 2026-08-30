package com.example.kafkatoy.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

// (status, created_at) 인덱스가 없으면 FOR UPDATE SKIP LOCKED가 정렬을 위해 PENDING 행
// 전체를 스캔하며 잠가버려, LIMIT으로 한 행만 가져와도 다른 폴러가 남은 행을 못 집는다.
// 이 인덱스로 대상 행만 인덱스 순서로 읽어 정확히 배치 크기만큼만 잠근다.
// (order-service의 OutboxEvent와 동일한 패턴 — payment-completed/payment-failed 발행에
// 재사용한다. 서비스 간 공유 모듈은 없어 그대로 복제했다.)
@Entity
@Table(name = "outbox_events",
        indexes = @Index(name = "idx_outbox_status_created", columnList = "status, created_at"))
public class OutboxEvent {

    @Id
    private String id;

    private String topic;
    private String aggregateId;
    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    private Instant createdAt;
    private Instant publishedAt;

    protected OutboxEvent() {}

    public static OutboxEvent pending(String topic, String aggregateId, String eventType, String payload) {
        OutboxEvent event = new OutboxEvent();
        event.id = UUID.randomUUID().toString();
        event.topic = topic;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.status = OutboxStatus.PENDING;
        event.createdAt = Instant.now();
        return event;
    }

    public void markPublished() {
        if (this.status != OutboxStatus.PENDING) {
            throw new IllegalStateException("Cannot publish OutboxEvent in status: " + this.status);
        }
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
    }

    public String getId() { return id; }
    public String getTopic() { return topic; }
    public String getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public OutboxStatus getStatus() { return status; }
}
