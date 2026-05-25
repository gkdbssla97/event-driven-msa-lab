package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.PaymentFailedEvent;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentFailedEventPublisher.class);

    private final KafkaTemplate<String, PaymentFailedEvent> kafkaTemplate;
    private final String paymentFailedTopic;

    public PaymentFailedEventPublisher(
            KafkaTemplate<String, PaymentFailedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.payment-failed}") String paymentFailedTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentFailedTopic = paymentFailedTopic;
    }

    public void publish(PaymentFailedEvent event) {
        try {
            kafkaTemplate.send(paymentFailedTopic, event.orderId(), event).get(10, TimeUnit.SECONDS);
            log.info("Kafka send acknowledged for payment-failed: orderId={}, topic={}", event.orderId(), paymentFailedTopic);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing payment-failed event", exception);
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to publish payment-failed event", exception);
        }
    }
}
