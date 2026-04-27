package com.example.kafkatoy.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class OrderCreatedEventTest {

    @Test
    void initialFactoryCreatesExpectedEventType() {
        OrderCreatedEvent event = OrderCreatedEvent.initial("order-1", "user-1");

        assertEquals("ORDER_CREATED", event.eventType());
        assertEquals("order-1", event.orderId());
        assertEquals("user-1", event.userId());
        assertNotNull(event.eventId());
        assertNotNull(event.timestamp());
    }
}
