package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final Map<String, PaymentCompletedEvent> processedOrders = new ConcurrentHashMap<>();

    public PaymentCompletedEvent process(OrderCreatedEvent event) {
        String orderId = event.orderId();

        if (processedOrders.containsKey(orderId)) {
            log.warn("Duplicate payment request detected, returning cached result: orderId={}", orderId);
            return processedOrders.get(orderId);
        }

        PaymentCompletedEvent result = PaymentCompletedEvent.initial(orderId, event.userId());
        processedOrders.put(orderId, result);
        return result;
    }
}
