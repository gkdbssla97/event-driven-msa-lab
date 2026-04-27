package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedEventListener {

    private final PaymentService paymentService;
    private final PaymentCompletedEventPublisher paymentCompletedEventPublisher;

    public OrderCreatedEventListener(
            PaymentService paymentService,
            PaymentCompletedEventPublisher paymentCompletedEventPublisher
    ) {
        this.paymentService = paymentService;
        this.paymentCompletedEventPublisher = paymentCompletedEventPublisher;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-created}")
    public void handle(OrderCreatedEvent event) {
        PaymentCompletedEvent completedEvent = paymentService.process(event);
        paymentCompletedEventPublisher.publish(completedEvent);
    }
}
