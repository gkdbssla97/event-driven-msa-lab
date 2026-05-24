package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    @Transactional
    public PaymentCompletedEvent process(OrderCreatedEvent event) {
        String orderId = event.orderId();

        if (paymentRepository.existsById(orderId)) {
            log.warn("Duplicate payment request, skipping: orderId={}", orderId);
            PaymentRecord existing = paymentRepository.findById(orderId).orElseThrow();
            return PaymentCompletedEvent.initial(existing.getOrderId(), existing.getUserId());
        }

        paymentRepository.save(PaymentRecord.success(orderId, event.userId()));
        return PaymentCompletedEvent.initial(orderId, event.userId());
    }
}
