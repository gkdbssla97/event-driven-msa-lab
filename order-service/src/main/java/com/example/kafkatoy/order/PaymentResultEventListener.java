package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import com.example.kafkatoy.contracts.PaymentFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentResultEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentResultEventListener.class);

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public PaymentResultEventListener(ObjectMapper objectMapper, OrderService orderService) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-completed}", groupId = "${spring.kafka.consumer.group-id}")
    public void handlePaymentCompleted(String payload) {
        PaymentCompletedEvent event = deserialize(payload, PaymentCompletedEvent.class);
        log.info("Payment completed: orderId={}", event.orderId());
        orderService.confirm(event.orderId());
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-failed}", groupId = "${spring.kafka.consumer.group-id}")
    public void handlePaymentFailed(String payload) {
        PaymentFailedEvent event = deserialize(payload, PaymentFailedEvent.class);
        log.info("Payment failed: orderId={}, reason={}", event.orderId(), event.failureReason());
        orderService.cancel(event.orderId());
    }

    private <T> T deserialize(String payload, Class<T> type) {
        try {
            return objectMapper.readValue(payload, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize event: " + type.getSimpleName(), e);
        }
    }
}
