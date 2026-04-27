package com.example.kafkatoy.order;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final String orderCreatedTopic;

    public OrderEventPublisher(
            KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
    }

    public void publish(OrderCreatedEvent event) {
        try {
            kafkaTemplate.send(orderCreatedTopic, event.orderId(), event).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing order-created event", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to publish order-created event", exception);
        }
    }
}
