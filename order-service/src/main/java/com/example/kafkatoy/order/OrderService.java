package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher orderEventPublisher;

    public OrderService(OrderRepository orderRepository, OrderEventPublisher orderEventPublisher) {
        this.orderRepository = orderRepository;
        this.orderEventPublisher = orderEventPublisher;
    }

    // TODO: Outbox Pattern 적용 전 — DB 저장 성공 후 Kafka 발행 실패 시 이벤트 유실 가능
    @Transactional
    public OrderCreateResponse create(OrderCreateRequest request) {
        String orderId = UUID.randomUUID().toString();

        Order order = Order.create(orderId, request.userId());
        orderRepository.save(order);

        OrderCreatedEvent event = OrderCreatedEvent.initial(orderId, request.userId());
        orderEventPublisher.publish(event);

        return new OrderCreateResponse(orderId, request.userId(), order.getStatus().name());
    }

    @Transactional
    public void confirm(String orderId) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.confirm();
            orderRepository.save(order);
        });
    }

    @Transactional
    public void cancel(String orderId) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.cancel();
            orderRepository.save(order);
        });
    }
}
