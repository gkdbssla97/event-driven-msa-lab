package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.example.kafkatoy.contracts.PaymentFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final SagaStateRepository sagaStateRepository;
    private final ObjectMapper objectMapper;
    private final String orderCreatedTopic;
    private final String paymentFailedTopic;

    public OrderService(OrderRepository orderRepository, OutboxRepository outboxRepository,
            SagaStateRepository sagaStateRepository, ObjectMapper objectMapper,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic,
            @Value("${app.kafka.topics.payment-failed}") String paymentFailedTopic) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.objectMapper = objectMapper;
        this.orderCreatedTopic = orderCreatedTopic;
        this.paymentFailedTopic = paymentFailedTopic;
    }

    @Transactional
    public OrderCreateResponse create(OrderCreateRequest request) {
        String orderId = UUID.randomUUID().toString();

        Order order = Order.create(orderId, request.userId());
        orderRepository.save(order);

        OrderCreatedEvent event = OrderCreatedEvent.initial(orderId, request.userId(), request.productId(), request.quantity());
        outboxRepository.save(OutboxEvent.pending(orderCreatedTopic, orderId, "ORDER_CREATED", serialize(event)));

        // 사가 상태도 주문·Outbox와 같은 로컬 트랜잭션에 묶는다 — 셋 다 order-service의
        // 같은 DB라 별도 보상 없이 원자적으로 함께 커밋/롤백된다.
        sagaStateRepository.save(SagaState.started(orderId));

        return new OrderCreateResponse(orderId, request.userId(), order.getStatus().name());
    }

    private String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event", e);
        }
    }

    @Transactional
    public void confirm(String orderId) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.confirm();
            orderRepository.save(order);
        });
        transitionSaga(orderId, SagaStatus.COMPLETED);
    }

    @Transactional
    public void cancel(String orderId, SagaStatus terminalStatus) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.cancel();
            orderRepository.save(order);
        });
        transitionSaga(orderId, terminalStatus);
    }

    @Transactional
    public void markReserved(String orderId) {
        transitionSaga(orderId, SagaStatus.RESERVED);
    }

    /**
     * 스위퍼가 판단한 "멈춘 사가"를 종결한다. {@link #failAndCompensate}에 TIMED_OUT을 위임한다.
     */
    @Transactional
    public boolean timeout(String orderId) {
        return failAndCompensate(orderId, SagaStatus.TIMED_OUT, "Saga timeout — no downstream response");
    }

    /**
     * 비정상 종료된 사가를 한 트랜잭션에서 종결한다:
     * 주문 취소 + 사가 종착 마킹(terminalStatus) + 재고 복원용 보상 이벤트를 Outbox에 적재.
     *
     * 스위퍼(TIMED_OUT)와 DLQ 복구(FAILED_POISON)가 공유하는 경로 — 감지 계기만 다를 뿐
     * "취소·마킹·보상"이라는 종결 액션은 동일하다.
     *
     * 셋이 order-service의 같은 로컬 트랜잭션이라 원자적으로 커밋된다 — 도중에 죽으면
     * 통째로 롤백되어 사가는 비종착 상태로 남고 다음 스윕/재전달에서 다시 잡힌다. Outbox에 적재된
     * 보상 이벤트는 OutboxPublisher가 at-least-once로 inventory-service에 전달하며,
     * inventory의 compensate()가 멱등(예약 없으면 no-op)이라 재고 예약 전 실패여도 안전하다.
     *
     * 이미 종착된 사가는 건드리지 않는다(멱등) — 지연된 스윕이나 재전달된 DLQ 메시지가
     * 같은 사가를 또 종결하려 해도 보상 이벤트는 한 번만 적재된다.
     */
    @Transactional
    public boolean failAndCompensate(String orderId, SagaStatus terminalStatus, String reason) {
        SagaState saga = sagaStateRepository.findById(orderId).orElse(null);
        if (saga == null || saga.getStatus().isTerminal()) {
            return false;
        }
        Order order = orderRepository.findById(orderId).orElse(null);
        if (order == null) {
            return false;
        }
        order.cancel();
        orderRepository.save(order);
        saga.transitionTo(terminalStatus);
        sagaStateRepository.save(saga);

        PaymentFailedEvent compensation = PaymentFailedEvent.of(orderId, order.getUserId(), reason);
        outboxRepository.save(OutboxEvent.pending(
                paymentFailedTopic, orderId, "PAYMENT_FAILED", serialize(compensation)));
        return true;
    }

    private void transitionSaga(String orderId, SagaStatus status) {
        sagaStateRepository.findById(orderId).ifPresent(saga -> {
            saga.transitionTo(status);
            sagaStateRepository.save(saga);
        });
    }
}
