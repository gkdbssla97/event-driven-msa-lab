package com.example.kafkatoy.websocket;

import com.example.kafkatoy.contracts.PaymentFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedEventListener.class);

    private final ObjectMapper objectMapper;
    private final PaymentUpdateBroadcaster broadcaster;

    public PaymentFailedEventListener(ObjectMapper objectMapper, PaymentUpdateBroadcaster broadcaster) {
        this.objectMapper = objectMapper;
        this.broadcaster = broadcaster;
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-failed}")
    public void handle(String payload) {
        try {
            PaymentFailedEvent event = objectMapper.readValue(payload, PaymentFailedEvent.class);
            log.info("Received payment-failed: orderId={}, reason={}", event.orderId(), event.failureReason());
            broadcaster.broadcastFailed(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payment-failed event", e);
        }
    }
}
