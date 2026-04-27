package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentCompletedEventPublisher {

    private final KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate;
    private final String paymentCompletedTopic;

    public PaymentCompletedEventPublisher(
            KafkaTemplate<String, PaymentCompletedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.payment-completed}") String paymentCompletedTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentCompletedTopic = paymentCompletedTopic;
    }

    public void publish(PaymentCompletedEvent event) {
        try {
            kafkaTemplate.send(paymentCompletedTopic, event.orderId(), event).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing payment-completed event", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to publish payment-completed event", exception);
        }
    }
}
