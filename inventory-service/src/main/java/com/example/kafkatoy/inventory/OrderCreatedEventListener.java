package com.example.kafkatoy.inventory;

import com.example.kafkatoy.contracts.InventoryFailedEvent;
import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedEventListener.class);

    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final InventoryReservedEventPublisher reservedPublisher;
    private final InventoryFailedEventPublisher failedPublisher;

    public OrderCreatedEventListener(ObjectMapper objectMapper,
                                     InventoryService inventoryService,
                                     InventoryReservedEventPublisher reservedPublisher,
                                     InventoryFailedEventPublisher failedPublisher) {
        this.objectMapper = objectMapper;
        this.inventoryService = inventoryService;
        this.reservedPublisher = reservedPublisher;
        this.failedPublisher = failedPublisher;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(String payload) {
        OrderCreatedEvent event = deserialize(payload);
        log.info("Received order-created: orderId={}, productId={}, quantity={}",
                event.orderId(), event.productId(), event.quantity());

        boolean reserved = inventoryService.reserve(event.orderId(), event.productId(), event.quantity());
        if (reserved) {
            reservedPublisher.publish(
                    InventoryReservedEvent.of(event.orderId(), event.userId(), event.productId(), event.quantity())
            );
        } else {
            failedPublisher.publish(
                    InventoryFailedEvent.of(event.orderId(), event.userId(), event.productId(),
                            event.quantity(), "Insufficient stock")
            );
        }
    }

    private OrderCreatedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize order-created event", e);
        }
    }
}
