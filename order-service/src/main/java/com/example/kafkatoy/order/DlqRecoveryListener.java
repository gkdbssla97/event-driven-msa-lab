package com.example.kafkatoy.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * DLQ 기반 사가 복구.
 *
 * inventory/payment-service가 메시지를 재시도 소진 후에도 처리하지 못하면
 * DeadLetterPublishingRecoverer가 원본 토픽 뒤에 ".DLQ"를 붙인 토픽으로 격리한다
 * (order-created.DLQ, inventory-reserved.DLQ). 사가 주인인 order-service가 이 DLQ들을
 * 구독해, poison 메시지에서 orderId만 뽑아 즉시 보상(주문 취소 + 재고 복원)한다.
 *
 * 스위퍼(SagaTimeoutSweeper)와 상호 보완:
 *  - 스위퍼   : 메시지가 "안 옴"(침묵/유실)을 타임아웃(기본 5분)으로 뒤늦게 감지.
 *  - DLQ 복구 : 메시지는 왔지만 "처리가 확정 실패"(poison)함을 재시도 소진 즉시 감지 → 초 단위 보상.
 * 둘 다 같은 {@link OrderService#failAndCompensate} 경로로 사가를 종착 실패로 몰지만,
 * 잡는 실패모드와 속도가 다르다.
 */
@Component
public class DlqRecoveryListener {

    private static final Logger log = LoggerFactory.getLogger(DlqRecoveryListener.class);

    private final OrderService orderService;
    private final ObjectMapper objectMapper;
    private final Counter recoveredCounter;

    public DlqRecoveryListener(OrderService orderService, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.orderService = orderService;
        this.objectMapper = objectMapper;
        this.recoveredCounter = Counter.builder("saga.dlq.recovered")
                .description("Number of sagas compensated from a dead-lettered (poison) message")
                .register(meterRegistry);
    }

    @KafkaListener(
            topics = {"${app.kafka.topics.order-created-dlq}", "${app.kafka.topics.inventory-reserved-dlq}"},
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        String orderId = extractOrderId(record.value());
        if (orderId == null) {
            // orderId를 못 뽑으면 보상 대상을 특정할 수 없다 — 로그만 남기고 흘려보낸다(무한 재처리 방지).
            log.error("DLQ message without parseable orderId, skipping: topic={}, value={}",
                    record.topic(), record.value());
            return;
        }
        String reason = "DLQ recovery — message dead-lettered from " + record.topic();
        // 보상은 멱등: 이미 종착된 사가면 no-op라 재전달된 DLQ 메시지에도 보상은 한 번만 적재된다.
        if (orderService.failAndCompensate(orderId, SagaStatus.FAILED_POISON, reason)) {
            recoveredCounter.increment();
            log.warn("Saga compensated from DLQ: orderId={}, dlqTopic={}", orderId, record.topic());
        }
    }

    /**
     * DLQ 메시지는 order-created / inventory-reserved 등 서로 다른 스키마지만 모두 orderId 필드를
     * 가진다. 전체를 역직렬화하지 않고 orderId만 뽑아 어떤 이벤트 타입이 격리됐든 동일하게 처리한다.
     */
    private String extractOrderId(String payload) {
        try {
            JsonNode orderId = objectMapper.readTree(payload).get("orderId");
            return orderId == null || orderId.isNull() ? null : orderId.asText();
        } catch (Exception e) {
            return null;
        }
    }
}
