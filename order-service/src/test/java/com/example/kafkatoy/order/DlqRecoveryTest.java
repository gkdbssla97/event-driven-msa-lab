package com.example.kafkatoy.order;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DlqRecoveryListener가 DLQ로 격리된 poison 메시지를 받아 해당 사가를 FAILED_POISON으로
 * 종결하고(주문 취소 + 재고 복원 보상 Outbox 적재), 이미 종착됐거나 orderId를 못 뽑는
 * 메시지에는 아무 것도 하지 않는지(멱등·안전) 검증한다.
 *
 * Kafka 배달 타이밍에 의존하지 않도록 리스너 메서드를 직접 호출한다 — 토픽 구독 배선은
 * @KafkaListener SpEL이, 복구 로직은 이 테스트가 각각 담당한다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                // 스위퍼/Outbox 스케줄러가 테스트 사가를 먼저 건드리지 않도록 지연을 크게
                "app.saga.sweep-interval-ms=600000",
                "app.outbox.initial-delay-ms=600000",
                "app.outbox.poll-interval-ms=600000"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-completed", "payment-failed", "inventory-failed", "inventory-reserved", "order-created.DLQ", "inventory-reserved.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class DlqRecoveryTest extends MySqlTestContainer {

    @Autowired
    private DlqRecoveryListener dlqRecoveryListener;
    @Autowired
    private OrderService orderService;
    @Autowired
    private SagaStateRepository sagaStateRepository;
    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    private OutboxRepository outboxRepository;

    private ConsumerRecord<String, String> dlqRecord(String topic, String orderId) {
        String payload = "{\"orderId\":\"" + orderId + "\",\"userId\":\"user-1\",\"productId\":\"product-A\",\"quantity\":1}";
        return new ConsumerRecord<>(topic, 0, 0L, orderId, payload);
    }

    @Test
    void deadLetter_stuckSaga_compensatesAndMarksPoison() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        String orderId = response.orderId();
        orderService.markReserved(orderId); // 결제가 처리 실패해 inventory-reserved.DLQ로 간 상황

        dlqRecoveryListener.onDeadLetter(dlqRecord("inventory-reserved.DLQ", orderId));

        assertThat(sagaStateRepository.findById(orderId).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.FAILED_POISON);
        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.CANCELED);

        boolean compensationEnqueued = outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING)
                .stream()
                .anyMatch(e -> e.getAggregateId().equals(orderId)
                        && "PAYMENT_FAILED".equals(e.getEventType())
                        && "payment-failed".equals(e.getTopic()));
        assertThat(compensationEnqueued)
                .as("DLQ로 격리된 사가에 대한 재고 복원 보상 이벤트가 Outbox에 적재되어야 한다")
                .isTrue();
    }

    @Test
    void deadLetter_alreadyTerminalSaga_isNoOp() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        String orderId = response.orderId();
        orderService.confirm(orderId); // 이미 COMPLETED로 종착

        // 뒤늦게 재전달된 DLQ 메시지가 도착해도 종착 상태를 되돌리거나 보상을 또 걸면 안 된다.
        dlqRecoveryListener.onDeadLetter(dlqRecord("order-created.DLQ", orderId));

        assertThat(sagaStateRepository.findById(orderId).orElseThrow().getStatus())
                .as("종착 상태(COMPLETED)는 DLQ 복구가 건드리지 않는다")
                .isEqualTo(SagaStatus.COMPLETED);
        boolean anyCompensation = outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING)
                .stream()
                .anyMatch(e -> e.getAggregateId().equals(orderId) && "PAYMENT_FAILED".equals(e.getEventType()));
        assertThat(anyCompensation)
                .as("이미 종착된 사가엔 보상 이벤트가 추가로 적재되면 안 된다")
                .isFalse();
    }

    @Test
    void deadLetter_unknownOrder_isNoOp() {
        // 존재하지 않는 사가면 예외 없이 그냥 통과해야 한다.
        dlqRecoveryListener.onDeadLetter(dlqRecord("order-created.DLQ", "no-such-order"));
    }

    @Test
    void deadLetter_unparseablePayload_isSkipped() {
        // orderId를 못 뽑는 메시지는 예외 없이 건너뛴다(무한 재처리 방지).
        dlqRecoveryListener.onDeadLetter(new ConsumerRecord<>("order-created.DLQ", 0, 0L, "k", "not-json"));
    }
}
