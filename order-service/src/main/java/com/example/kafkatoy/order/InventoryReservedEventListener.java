package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * inventory-reserved는 원래 payment-service만 구독했지만, order-service도 구독해
 * SagaState를 RESERVED로 갱신한다. Order.status는 건드리지 않는다 — 주문의 성공
 * 여부는 결제 결과가 결정하고, 이 리스너는 오직 "사가가 여기까지는 왔다"는
 * 관측용 중간 지점만 기록한다.
 */
@Component
public class InventoryReservedEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservedEventListener.class);

    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public InventoryReservedEventListener(ObjectMapper objectMapper, OrderService orderService) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(topics = "${app.kafka.topics.inventory-reserved}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(String payload) {
        InventoryReservedEvent event = deserialize(payload);
        log.info("Inventory reserved (saga milestone): orderId={}", event.orderId());
        orderService.markReserved(event.orderId());
    }

    private InventoryReservedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, InventoryReservedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize inventory-reserved event", e);
        }
    }
}
