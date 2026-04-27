package com.example.kafkatoy.contracts;

import java.time.Instant;

public record OrderCreatedEvent(
        String eventId,
        String eventType,
        String orderId,
        String userId,
        Instant timestamp
) implements DomainEvent {
    public static OrderCreatedEvent initial(String orderId, String userId) {
        return new OrderCreatedEvent(
                java.util.UUID.randomUUID().toString(),
                "ORDER_CREATED",
                orderId,
                userId,
                Instant.now()
        );
    }
}
