package com.example.kafkatoy.contracts;

import java.time.Instant;
import java.util.UUID;

public record InventoryReservedEvent(
        String eventId,
        String eventType,
        String orderId,
        String userId,
        String productId,
        int quantity,
        Instant timestamp
) implements DomainEvent {
    public static InventoryReservedEvent of(String orderId, String userId, String productId, int quantity) {
        return new InventoryReservedEvent(
                UUID.randomUUID().toString(),
                "INVENTORY_RESERVED",
                orderId,
                userId,
                productId,
                quantity,
                Instant.now()
        );
    }
}
