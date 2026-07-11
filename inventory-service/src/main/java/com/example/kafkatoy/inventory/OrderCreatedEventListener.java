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
    private final InventoryStore inventoryStore;
    private final InventoryReservedEventPublisher reservedPublisher;
    private final InventoryFailedEventPublisher failedPublisher;

    public OrderCreatedEventListener(ObjectMapper objectMapper,
                                     InventoryStore inventoryStore,
                                     InventoryReservedEventPublisher reservedPublisher,
                                     InventoryFailedEventPublisher failedPublisher) {
        this.objectMapper = objectMapper;
        this.inventoryStore = inventoryStore;
        this.reservedPublisher = reservedPublisher;
        this.failedPublisher = failedPublisher;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(String payload) {
        OrderCreatedEvent event = deserialize(payload);
        log.info("Received order-created: orderId={}, productId={}, quantity={}",
                event.orderId(), event.productId(), event.quantity());

        switch (inventoryStore.claim(event.orderId())) {
            case CLAIMED -> process(event);
            // 중복 재전달: 처리를 다시 하지 않고, 첫 처리 때의 결과 이벤트를 그대로 재발행한다.
            // (스킵하면 첫 발행이 유실됐을 때 사가가 영영 멈춘다 — "스킵"이 아니라 "replay")
            case DUPLICATE_RESERVED -> {
                log.warn("Duplicate order-created (already reserved), replaying inventory-reserved: orderId={}", event.orderId());
                publishReserved(event);
            }
            case DUPLICATE_FAILED -> {
                log.warn("Duplicate order-created (already failed), replaying inventory-failed: orderId={}", event.orderId());
                publishFailed(event);
            }
            // 첫 처리가 결과를 남기기 전에 죽은 경우. 재발행할 근거가 없어 스킵한다(알려진 잔여 한계).
            case IN_PROGRESS -> log.warn(
                    "Duplicate order-created while first attempt in-progress/crashed, skipping: orderId={}", event.orderId());
        }
    }

    private void process(OrderCreatedEvent event) {
        boolean reserved = inventoryStore.reserve(event.orderId(), event.productId(), event.quantity());
        inventoryStore.markOutcome(event.orderId(), reserved);
        if (reserved) {
            publishReserved(event);
        } else {
            publishFailed(event);
        }
    }

    private void publishReserved(OrderCreatedEvent event) {
        reservedPublisher.publish(
                InventoryReservedEvent.of(event.orderId(), event.userId(), event.productId(), event.quantity()));
    }

    private void publishFailed(OrderCreatedEvent event) {
        failedPublisher.publish(
                InventoryFailedEvent.of(event.orderId(), event.userId(), event.productId(),
                        event.quantity(), "Insufficient stock"));
    }

    private OrderCreatedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize order-created event", e);
        }
    }
}
