package com.example.kafkatoy.inventory;

import com.example.kafkatoy.contracts.PaymentFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 보상 트랜잭션(Compensating Transaction).
 * 결제 실패 시 inventory-service가 이미 차감한 재고를 복원한다.
 * payment-failed 이벤트에는 productId/quantity가 없으므로
 * orderId를 키로 하는 예약 내역 캐시에서 조회한다.
 */
@Component
public class PaymentFailedEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedEventListener.class);

    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final ReservationStore reservationStore;

    public PaymentFailedEventListener(ObjectMapper objectMapper,
                                      InventoryService inventoryService,
                                      ReservationStore reservationStore) {
        this.objectMapper = objectMapper;
        this.inventoryService = inventoryService;
        this.reservationStore = reservationStore;
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-failed}", groupId = "${spring.kafka.consumer.group-id}")
    public void handle(String payload) {
        PaymentFailedEvent event = deserialize(payload);
        log.info("Received payment-failed (compensation trigger): orderId={}", event.orderId());

        reservationStore.find(event.orderId()).ifPresent(reservation -> {
            inventoryService.release(reservation.productId(), reservation.quantity());
            reservationStore.remove(event.orderId());
            log.info("Compensation complete: stock restored for orderId={}, productId={}",
                    event.orderId(), reservation.productId());
        });
    }

    private PaymentFailedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, PaymentFailedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize payment-failed event", e);
        }
    }
}
