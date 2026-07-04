package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final Counter paymentNewCounter;
    private final Counter paymentDuplicateCounter;

    public PaymentService(PaymentRepository paymentRepository, MeterRegistry meterRegistry) {
        this.paymentRepository = paymentRepository;
        this.paymentNewCounter = Counter.builder("payment.processed")
                .tag("result", "new").register(meterRegistry);
        this.paymentDuplicateCounter = Counter.builder("payment.processed")
                .tag("result", "duplicate").register(meterRegistry);
    }

    @Transactional
    public PaymentCompletedEvent process(InventoryReservedEvent event) {
        String orderId = event.orderId();

        if (paymentRepository.existsById(orderId)) {
            log.warn("Duplicate payment request, skipping: orderId={}", orderId);
            paymentDuplicateCounter.increment();
            PaymentRecord existing = paymentRepository.findById(orderId).orElseThrow();
            return PaymentCompletedEvent.initial(existing.getOrderId(), existing.getUserId());
        }

        paymentRepository.save(PaymentRecord.success(orderId, event.userId()));
        paymentNewCounter.increment();
        return PaymentCompletedEvent.initial(orderId, event.userId());
    }
}
