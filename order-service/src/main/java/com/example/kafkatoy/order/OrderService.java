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
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository, OutboxRepository outboxRepository,
            ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderCreateResponse create(OrderCreateRequest request) {
        String orderId = UUID.randomUUID().toString();

        Order order = Order.create(orderId, request.userId());
        orderRepository.save(order);

        OrderCreatedEvent event = OrderCreatedEvent.initial(orderId, request.userId());
        outboxRepository.save(OutboxEvent.pending(orderId, "ORDER_CREATED", serialize(event)));

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
    }

    @Transactional
    public void cancel(String orderId) {
        orderRepository.findById(orderId).ifPresent(order -> {
            order.cancel();
            orderRepository.save(order);
        });
    }
}
