package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PaymentService.process()/recordFailure()가 Kafka로 직접 발행하지 않고 같은 트랜잭션에서
 * Outbox에 적재하는지 검증한다. 백그라운드 폴러는 큰 딜레이로 꺼서 폴러가 끼어들기 전
 * PENDING 상태를 안정적으로 관찰한다 — SKIP LOCKED 폴링 메커니즘 자체는 order-service의
 * SkipLockedOutboxPublisherTest가 이미 검증했으므로 여기서 재검증하지 않는다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                "app.outbox.initial-delay-ms=600000",
                "app.outbox.poll-interval-ms=600000"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"inventory-reserved", "payment-completed", "payment-failed", "order-created.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class PaymentServiceOutboxTest extends MySqlTestContainer {

    @Autowired
    private PaymentService paymentService;
    @Autowired
    private OutboxRepository outboxRepository;

    @Test
    void process_enqueuesPaymentCompletedOutboxEvent_exactlyOnce() {
        String orderId = UUID.randomUUID().toString();
        InventoryReservedEvent event = InventoryReservedEvent.of(orderId, "user-1", "product-A", 1);

        paymentService.process(event);
        paymentService.process(event); // 재전달 시뮬레이션 — PaymentRecord 게이트에 걸려 재적재 안 됨

        List<OutboxEvent> pending = outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);
        long matching = pending.stream()
                .filter(e -> e.getAggregateId().equals(orderId) && "PAYMENT_COMPLETED".equals(e.getEventType()))
                .count();

        assertThat(matching).as("중복 호출이어도 outbox 행은 하나만 적재돼야 한다").isEqualTo(1);
        OutboxEvent enqueued = pending.stream()
                .filter(e -> e.getAggregateId().equals(orderId))
                .findFirst().orElseThrow();
        assertThat(enqueued.getTopic()).isEqualTo("payment-completed");
        assertThat(enqueued.getPayload()).contains(orderId);
    }

    @Test
    void recordFailure_enqueuesPaymentFailedOutboxEvent() {
        String orderId = UUID.randomUUID().toString();

        paymentService.recordFailure(orderId, "user-1", "Payment gateway timeout");

        boolean enqueued = outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING).stream()
                .anyMatch(e -> e.getAggregateId().equals(orderId)
                        && "PAYMENT_FAILED".equals(e.getEventType())
                        && "payment-failed".equals(e.getTopic()));

        assertThat(enqueued).as("실패 처리도 Outbox를 통해 적재돼야 한다").isTrue();
    }
}
