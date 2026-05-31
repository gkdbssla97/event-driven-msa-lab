package com.example.kafkatoy.websocket;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCompletedEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedEventListener.class);

    private final ObjectMapper objectMapper;
    private final PaymentUpdateBroadcaster broadcaster;

    public PaymentCompletedEventListener(ObjectMapper objectMapper, PaymentUpdateBroadcaster broadcaster) {
        this.objectMapper = objectMapper;
        this.broadcaster = broadcaster;
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-completed}")
    public void handle(String payload) {
        try {
            PaymentCompletedEvent event = objectMapper.readValue(payload, PaymentCompletedEvent.class);
            log.info("Received payment-completed: orderId={}", event.orderId());
            broadcaster.broadcastCompleted(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize payment-completed event", e);
        }
    }
}
