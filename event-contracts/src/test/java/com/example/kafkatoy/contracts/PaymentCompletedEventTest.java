package com.example.kafkatoy.contracts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PaymentCompletedEventTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void initialFactoryCreatesExpectedEventType() {
        PaymentCompletedEvent event = PaymentCompletedEvent.initial("order-1", "user-1");

        assertEquals("PAYMENT_COMPLETED", event.eventType());
        assertEquals("order-1", event.orderId());
        assertEquals("user-1", event.userId());
        assertNotNull(event.eventId());
        assertNotNull(event.timestamp());
    }

    @Test
    void eventCanBeSerializedAndDeserialized() throws Exception {
        PaymentCompletedEvent source = new PaymentCompletedEvent(
                "event-1",
                "PAYMENT_COMPLETED",
                "order-1",
                "user-1",
                Instant.parse("2026-04-28T00:00:00Z")
        );

        String json = objectMapper.writeValueAsString(source);
        PaymentCompletedEvent restored = objectMapper.readValue(json, PaymentCompletedEvent.class);

        assertEquals(source, restored);
    }
}
