package com.example.kafkatoy.contracts;

import java.time.Instant;
import java.util.UUID;

public record InventoryFailedEvent(
        String eventId,
        String eventType,
        String orderId,
        String userId,
        String productId,
        int requestedQuantity,
        String failureReason,
        Instant timestamp
) implements DomainEvent {
    public static InventoryFailedEvent of(String orderId, String userId, String productId,
                                          int requestedQuantity, String failureReason) {
        return new InventoryFailedEvent(
                UUID.randomUUID().toString(),
                "INVENTORY_FAILED",
                orderId,
                userId,
                productId,
                requestedQuantity,
                failureReason,
                Instant.now()
        );
    }
}
