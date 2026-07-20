package com.example.kafkatoy.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SagaTimeoutSweeper가 STARTED/RESERVED로 멈춘 사가를 찾아 자동으로 주문 취소 +
 * TIMED_OUT 마킹 + 재고 복원 보상 이벤트(Outbox) 적재까지 수행하는지 검증한다.
 *
 * 스위퍼가 timeout-ms=0으로 모든 비종착 사가를 쓸어가므로, 다른 테스트 데이터를
 * 건드리지 않도록 전용 MySQL 컨테이너로 격리한다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                // 모든 비종착 사가를 즉시 "멈춤"으로 판정
                "app.saga.timeout-ms=0",
                // 자동 스윕/Outbox 발행을 꺼서 테스트가 sweep 시점을 직접 통제 (huge delay)
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
class SagaTimeoutSweeperTest {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    static {
        MYSQL.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
    }

    @Autowired
    private OrderService orderService;
    @Autowired
    private SagaTimeoutSweeper sweeper;
    @Autowired
    private SagaStateRepository sagaStateRepository;
    @Autowired
    private OrderRepository orderRepository;
    @Autowired
    private OutboxRepository outboxRepository;

    @Test
    void sweep_stuckSaga_cancelsOrderAndEnqueuesCompensation() throws InterruptedException {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        String orderId = response.orderId();
        orderService.markReserved(orderId); // RESERVED 상태에서 멈춘 사가

        Thread.sleep(50); // updatedAt이 threshold(now)보다 확실히 과거가 되도록

        sweeper.sweepStuckSagas();

        // 사가는 TIMED_OUT 종착, 주문은 CANCELED
        assertThat(sagaStateRepository.findById(orderId).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.TIMED_OUT);
        assertThat(orderRepository.findById(orderId).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.CANCELED);

        // 재고 복원용 보상 이벤트(PAYMENT_FAILED)가 payment-failed 토픽으로 Outbox에 적재됨
        boolean compensationEnqueued = outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING)
                .stream()
                .anyMatch(e -> e.getAggregateId().equals(orderId)
                        && "PAYMENT_FAILED".equals(e.getEventType())
                        && "payment-failed".equals(e.getTopic()));
        assertThat(compensationEnqueued)
                .as("멈춘 사가에 대한 보상 이벤트가 Outbox에 적재되어야 한다")
                .isTrue();
    }

    @Test
    void sweep_ignoresTerminalSagas() throws InterruptedException {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        String orderId = response.orderId();
        orderService.confirm(orderId); // COMPLETED로 이미 종착

        Thread.sleep(50);
        sweeper.sweepStuckSagas();

        // 종착 상태는 스위퍼가 건드리지 않는다 → COMPLETED 유지
        assertThat(sagaStateRepository.findById(orderId).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.COMPLETED);
    }

    @Test
    void timeout_returnsFalseForUnknownOrder() {
        assertThat(orderService.timeout("no-such-order")).isFalse();
    }

    @Test
    void sweep_terminalTransitionGuard_keepsTimedOutEvenIfCompensatedArrivesLater() throws InterruptedException {
        OrderCreateResponse response = orderService.create(new OrderCreateRequest("user-1", "product-A", 1));
        String orderId = response.orderId();
        Thread.sleep(50);
        sweeper.sweepStuckSagas(); // → TIMED_OUT

        // 뒤늦게 진짜 payment-failed가 도착해 cancel(COMPENSATED)이 불려도 상태가 바뀌면 안 된다.
        orderService.cancel(orderId, SagaStatus.COMPENSATED);

        assertThat(sagaStateRepository.findById(orderId).orElseThrow().getStatus())
                .as("TIMED_OUT은 종착 상태라 이후 COMPENSATED 전이가 무시되어야 한다")
                .isEqualTo(SagaStatus.TIMED_OUT);
    }

    @Test
    void sweep_noStuckSagas_isNoOp() {
        // 아무 사가도 없거나 전부 종착이면 예외 없이 그냥 통과해야 한다.
        List<SagaState> before = sagaStateRepository.findByStatusInAndUpdatedAtBefore(
                List.of(SagaStatus.STARTED, SagaStatus.RESERVED), java.time.Instant.now());
        // (선행 테스트가 남긴 비종착 사가가 없을 수도, 있을 수도 있으니 개수는 단정하지 않는다)
        sweeper.sweepStuckSagas(); // 예외 없이 완료되면 성공
        assertThat(before).isNotNull();
    }
}
