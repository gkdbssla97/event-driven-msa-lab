package com.example.kafkatoy.contracts;

import java.time.Instant;
import java.util.UUID;

public record PaymentCompletedEvent(
        String eventId,
        String eventType,
        String orderId,
        String userId,
        Instant timestamp
) implements DomainEvent {
    public static PaymentCompletedEvent initial(String orderId, String userId) {
        return new PaymentCompletedEvent(
                UUID.randomUUID().toString(),
                "PAYMENT_COMPLETED",
                orderId,
                userId,
                Instant.now()
        );
    }
}
