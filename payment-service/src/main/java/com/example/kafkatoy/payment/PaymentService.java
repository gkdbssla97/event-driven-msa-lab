package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final long unitPriceKrw;
    private final Counter paymentNewCounter;
    private final Counter paymentDuplicateCounter;
    private final Counter ledgerRecordedCounter;

    public PaymentService(PaymentRepository paymentRepository, LedgerEntryRepository ledgerEntryRepository,
            MeterRegistry meterRegistry, @Value("${app.payment.unit-price-krw:10000}") long unitPriceKrw) {
        this.paymentRepository = paymentRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.unitPriceKrw = unitPriceKrw;
        this.paymentNewCounter = Counter.builder("payment.processed")
                .tag("result", "new").register(meterRegistry);
        this.paymentDuplicateCounter = Counter.builder("payment.processed")
                .tag("result", "duplicate").register(meterRegistry);
        this.ledgerRecordedCounter = Counter.builder("payment.ledger")
                .tag("type", "paid").register(meterRegistry);
    }

    /**
     * 결제 처리 + 원장 기록을 한 트랜잭션에서 수행한다.
     *
     * 알려진 한계: 이 메서드가 커밋된 뒤, 호출자(InventoryReservedEventListener)가 트랜잭션 밖에서
     * payment-completed를 발행한다. 그 발행이 실패하면 호출자는 payment-failed로 대체 발행하지만,
     * 이 원장에는 이미 PAID 엔트리가 커밋돼 있어 "원장상 결제됨 vs 사가상 실패"가 어긋날 수 있다.
     * payment-service에도 Outbox 패턴을 적용해야 근본적으로 해소되며, 이번 증분의 스코프 밖이다.
     */
    @Transactional
    public PaymentCompletedEvent process(InventoryReservedEvent event) {
        String orderId = event.orderId();

        PaymentRecord existing = paymentRepository.findById(orderId).orElse(null);
        if (existing != null) {
            log.warn("Duplicate payment request, skipping: orderId={}", orderId);
            paymentDuplicateCounter.increment();
            return PaymentCompletedEvent.initial(existing.getOrderId(), existing.getUserId());
        }

        paymentRepository.save(PaymentRecord.success(orderId, event.userId()));
        paymentNewCounter.increment();

        // 실제 상품 카탈로그 대신 고정 단가로 계산한다 — inventory-service의
        // app.inventory.initial-stock과 같은 선상의 단순화(ledger-lite 스코프).
        long amount = (long) event.quantity() * unitPriceKrw;
        String ledgerId = LedgerEntry.ledgerId(orderId, LedgerEntryType.PAID);
        if (ledgerEntryRepository.findById(ledgerId).isEmpty()) {
            ledgerEntryRepository.save(LedgerEntry.paid(orderId, event.userId(), amount));
            ledgerRecordedCounter.increment();
        }

        return PaymentCompletedEvent.initial(orderId, event.userId());
    }
}
