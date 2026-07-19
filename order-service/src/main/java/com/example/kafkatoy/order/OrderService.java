package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final SagaStateRepository sagaStateRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository, OutboxRepository outboxRepository,
            SagaStateRepository sagaStateRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.sagaStateRepository = sagaStateRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderCreateResponse create(OrderCreateRequest request) {
        String orderId = UUID.randomUUID().toString();

        Order order = Order.create(orderId, request.userId());
        orderRepository.save(order);

        OrderCreatedEvent event = OrderCreatedEvent.initial(orderId, request.userId(), request.productId(), request.quantity());
        outboxRepository.save(OutboxEvent.pending(orderId, "ORDER_CREATED", serialize(event)));

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

    private void transitionSaga(String orderId, SagaStatus status) {
        sagaStateRepository.findById(orderId).ifPresent(saga -> {
            saga.transitionTo(status);
            sagaStateRepository.save(saga);
        });
    }
}
