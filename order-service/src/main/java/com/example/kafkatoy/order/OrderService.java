package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final OrderEventPublisher orderEventPublisher;

    public OrderService(OrderEventPublisher orderEventPublisher) {
        this.orderEventPublisher = orderEventPublisher;
    }

    public OrderCreateResponse create(OrderCreateRequest request) {
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent event = OrderCreatedEvent.initial(orderId, request.userId());

        orderEventPublisher.publish(event);

        return new OrderCreateResponse(orderId, request.userId(), "ACCEPTED");
    }
}
