package com.example.kafkatoy.contracts;

import java.time.Instant;

public interface DomainEvent {
    String eventId();

    String eventType();

    String orderId();

    String userId();

    Instant timestamp();
}
