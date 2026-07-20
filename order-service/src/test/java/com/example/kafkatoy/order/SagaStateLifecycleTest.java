package com.example.kafkatoy.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SagaState가 사가의 각 단계(주문 생성 → 재고 예약 → 결제 성공/실패, 또는 재고 부족)를
 * Order.status와 별개로 정확히 추적하는지, 그리고 종착 상태 이후로는 더 이상 바뀌지
 * 않는지(재전달·순서 뒤바뀜에 안전한지) 검증한다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                // 백그라운드 Outbox 스케줄러가 테스트 중간에 끼어들지 않도록 지연을 크게
                "app.outbox.initial-delay-ms=600000",
                "app.outbox.poll-interval-ms=600000"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-completed", "payment-failed", "inventory-failed", "inventory-reserved", "order-created.DLQ", "inventory-reserved.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class SagaStateLifecycleTest extends MySqlTestContainer {

    @Autowired
    private OrderService orderService;

    @Autowired
    private SagaStateRepository sagaStateRepository;

    @Test
    void create_startsNewSaga() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));

        SagaState saga = sagaStateRepository.findById(response.orderId()).orElseThrow();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
    }

    @Test
    void markReserved_transitionsToReserved() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));

        orderService.markReserved(response.orderId());

        SagaState saga = sagaStateRepository.findById(response.orderId()).orElseThrow();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.RESERVED);
    }

    @Test
    void confirm_transitionsToCompleted() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        orderService.markReserved(response.orderId());

        orderService.confirm(response.orderId());

        SagaState saga = sagaStateRepository.findById(response.orderId()).orElseThrow();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void cancel_withInsufficientStock_recordsCorrectTerminalStatus() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));

        // 재고 부족은 애초에 차감이 안 됐으니 RESERVED를 거치지 않고 바로 실패한다.
        orderService.cancel(response.orderId(), SagaStatus.FAILED_INSUFFICIENT_STOCK);

        SagaState saga = sagaStateRepository.findById(response.orderId()).orElseThrow();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.FAILED_INSUFFICIENT_STOCK);
    }

    @Test
    void cancel_afterReservation_recordsCompensated() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        orderService.markReserved(response.orderId());

        // 결제 실패는 이미 예약된 재고를 보상해야 하는 경우다.
        orderService.cancel(response.orderId(), SagaStatus.COMPENSATED);

        SagaState saga = sagaStateRepository.findById(response.orderId()).orElseThrow();
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    void terminalState_ignoresFurtherTransitions() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        orderService.confirm(response.orderId()); // COMPLETED로 종착

        // 종착 이후에 재전달된 inventory-reserved가 뒤늦게 도착해도 상태가 되돌아가면 안 된다.
        orderService.markReserved(response.orderId());

        SagaState saga = sagaStateRepository.findById(response.orderId()).orElseThrow();
        assertThat(saga.getStatus())
                .as("종착 상태(COMPLETED) 이후의 전이는 무시되어야 한다")
                .isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void findStuckSagas_includesNonTerminalSagas() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));

        // 실제 스위퍼는 Instant.now().minus(5, MINUTES)를 임계값으로 "그 이전에 갱신된 것"을 찾는다.
        // 테스트에서는 방금 만든 사가가 "가상의 미래 시점(60초 뒤)"에서 봤을 때 여전히 멈춰있는
        // 것으로 잡히는지를 같은 방식으로 검증한다.
        Instant futureThreshold = Instant.now().plusSeconds(60);
        List<SagaState> stuck = sagaStateRepository.findByStatusInAndUpdatedAtBefore(
                List.of(SagaStatus.STARTED, SagaStatus.RESERVED), futureThreshold);

        assertThat(stuck).extracting(SagaState::getSagaId).contains(response.orderId());
    }

    @Test
    void findStuckSagas_excludesTerminalSagas() {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        orderService.confirm(response.orderId());

        Instant futureThreshold = Instant.now().plusSeconds(60);
        List<SagaState> stuck = sagaStateRepository.findByStatusInAndUpdatedAtBefore(
                List.of(SagaStatus.STARTED, SagaStatus.RESERVED), futureThreshold);

        assertThat(stuck).extracting(SagaState::getSagaId).doesNotContain(response.orderId());
    }
}
