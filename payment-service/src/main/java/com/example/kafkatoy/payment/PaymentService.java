package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    public PaymentCompletedEvent process(OrderCreatedEvent event) {
        return PaymentCompletedEvent.initial(event.orderId(), event.userId());
    }
}
