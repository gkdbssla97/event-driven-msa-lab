package com.example.kafkatoy.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PaymentFailedEventTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void factoryCreatesExpectedEventType() {
        PaymentFailedEvent event = PaymentFailedEvent.of("order-1", "user-1", "INSUFFICIENT_BALANCE");

        assertEquals("PAYMENT_FAILED", event.eventType());
        assertEquals("order-1", event.orderId());
        assertEquals("user-1", event.userId());
        assertEquals("INSUFFICIENT_BALANCE", event.failureReason());
        assertNotNull(event.eventId());
        assertNotNull(event.timestamp());
    }

    @Test
    void eventCanBeSerializedAndDeserialized() throws Exception {
        PaymentFailedEvent source = new PaymentFailedEvent(
                "event-1",
                "PAYMENT_FAILED",
                "order-1",
                "user-1",
                "INSUFFICIENT_BALANCE",
                Instant.parse("2026-04-28T00:00:00Z")
        );

        String json = objectMapper.writeValueAsString(source);
        PaymentFailedEvent restored = objectMapper.readValue(json, PaymentFailedEvent.class);

        assertEquals(source, restored);
    }
}
