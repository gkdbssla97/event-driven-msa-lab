package com.example.kafkatoy.contracts;

import java.time.Instant;
import java.util.UUID;

public record PaymentFailedEvent(
        String eventId,
        String eventType,
        String orderId,
        String userId,
        String failureReason,
        Instant timestamp
) implements DomainEvent {
    public static PaymentFailedEvent of(String orderId, String userId, String failureReason) {
        return new PaymentFailedEvent(
                UUID.randomUUID().toString(),
                "PAYMENT_FAILED",
                orderId,
                userId,
                failureReason,
                Instant.now()
        );
    }
}
