package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import com.example.kafkatoy.contracts.PaymentFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryReservedEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservedEventListener.class);
    private static final int MAX_RETRY = 3;

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final PaymentCompletedEventPublisher paymentCompletedEventPublisher;
    private final PaymentFailedEventPublisher paymentFailedEventPublisher;

    public InventoryReservedEventListener(
            ObjectMapper objectMapper,
            PaymentService paymentService,
            PaymentCompletedEventPublisher paymentCompletedEventPublisher,
            PaymentFailedEventPublisher paymentFailedEventPublisher
    ) {
        this.objectMapper = objectMapper;
        this.paymentService = paymentService;
        this.paymentCompletedEventPublisher = paymentCompletedEventPublisher;
        this.paymentFailedEventPublisher = paymentFailedEventPublisher;
    }

    @KafkaListener(topics = "${app.kafka.topics.inventory-reserved}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(String payload) {
        InventoryReservedEvent event = deserialize(payload);
        log.info("Received inventory-reserved: orderId={}", event.orderId());

        try {
            PaymentCompletedEvent completedEvent = processWithRetry(event);
            paymentCompletedEventPublisher.publish(completedEvent);
            log.info("Published payment-completed: orderId={}", completedEvent.orderId());
        } catch (Exception e) {
            log.error("Payment failed after {} retries, publishing payment-failed: orderId={}", MAX_RETRY, event.orderId());
            paymentFailedEventPublisher.publish(
                    PaymentFailedEvent.of(event.orderId(), event.userId(), e.getMessage())
            );
        }
    }

    private PaymentCompletedEvent processWithRetry(InventoryReservedEvent event) {
        int attempt = 0;
        while (true) {
            try {
                return paymentService.process(event);
            } catch (Exception e) {
                attempt++;
                if (attempt >= MAX_RETRY) throw e;
                log.warn("Payment processing failed, retrying ({}/{}): orderId={}", attempt, MAX_RETRY, event.orderId());
                sleep(attempt);
            }
        }
    }

    private void sleep(int attempt) {
        try {
            Thread.sleep(1000L * attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private InventoryReservedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, InventoryReservedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize inventory-reserved event", e);
        }
    }
}
