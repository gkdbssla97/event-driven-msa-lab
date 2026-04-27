package com.example.kafkatoy.websocket;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCompletedEventListener {

    private final PaymentUpdateBroadcaster paymentUpdateBroadcaster;

    public PaymentCompletedEventListener(PaymentUpdateBroadcaster paymentUpdateBroadcaster) {
        this.paymentUpdateBroadcaster = paymentUpdateBroadcaster;
    }

    @KafkaListener(topics = "${app.kafka.topics.payment-completed}")
    public void handle(PaymentCompletedEvent event) {
        paymentUpdateBroadcaster.broadcast(event);
    }
}
