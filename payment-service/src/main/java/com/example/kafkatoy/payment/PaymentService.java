package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import com.example.kafkatoy.contracts.PaymentFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final long unitPriceKrw;
    private final String paymentCompletedTopic;
    private final String paymentFailedTopic;
    private final Counter paymentNewCounter;
    private final Counter paymentDuplicateCounter;
    private final Counter ledgerRecordedCounter;

    public PaymentService(PaymentRepository paymentRepository, LedgerEntryRepository ledgerEntryRepository,
            OutboxRepository outboxRepository, ObjectMapper objectMapper, MeterRegistry meterRegistry,
            @Value("${app.payment.unit-price-krw:10000}") long unitPriceKrw,
            @Value("${app.kafka.topics.payment-completed}") String paymentCompletedTopic,
            @Value("${app.kafka.topics.payment-failed}") String paymentFailedTopic) {
        this.paymentRepository = paymentRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.unitPriceKrw = unitPriceKrw;
        this.paymentCompletedTopic = paymentCompletedTopic;
        this.paymentFailedTopic = paymentFailedTopic;
        this.paymentNewCounter = Counter.builder("payment.processed")
                .tag("result", "new").register(meterRegistry);
        this.paymentDuplicateCounter = Counter.builder("payment.processed")
                .tag("result", "duplicate").register(meterRegistry);
        this.ledgerRecordedCounter = Counter.builder("payment.ledger")
                .tag("type", "paid").register(meterRegistry);
    }

    /**
     * 결제 처리 + 원장 기록 + 발행 결정(Outbox 적재)을 한 트랜잭션에서 수행한다.
     *
     * 예전엔 이 메서드가 이벤트를 반환하고 호출자가 트랜잭션 밖에서 직접 Kafka로 발행했다
     * — 그 발행이 실패하면 호출자가 payment-failed로 대체 발행하는데, DB엔 이미 이 메서드가
     * 커밋한 PAID 원장이 남아 "원장상 결제됨 vs 사가상 실패"가 어긋나는 dual-write 갭이 있었다.
     * 발행 결정 자체를 이 트랜잭션 안(Outbox 적재)으로 옮겨 그 갭을 없앴다 — 커밋되는 것과
     * 실제 발행될 이벤트가 항상 같아진다.
     *
     * 중복(이미 처리된 orderId)이어도 더 이상 재발행하지 않는다: 최초 처리 때 이미 Outbox에
     * 적재됐고, 그 행은 프로세스가 죽어도 살아남아 폴러가 결국 발행을 보장하므로 수동 재발행이
     * 불필요해졌다 — Outbox 도입이 없앤 안전장치다.
     */
    @Transactional
    public void process(InventoryReservedEvent event) {
        String orderId = event.orderId();

        PaymentRecord existing = paymentRepository.findById(orderId).orElse(null);
        if (existing != null) {
            log.warn("Duplicate payment request, skipping: orderId={}", orderId);
            paymentDuplicateCounter.increment();
            return;
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

        PaymentCompletedEvent completed = PaymentCompletedEvent.initial(orderId, event.userId());
        outboxRepository.save(OutboxEvent.pending(
                paymentCompletedTopic, orderId, "PAYMENT_COMPLETED", serialize(completed)));
    }

    /**
     * 재시도 소진 후 결제를 포기할 때 호출자(InventoryReservedEventListener)가 사용한다.
     * 이것도 트랜잭션 안에서 Outbox로 적재한다 — 실패 처리 역시 발행 신뢰성이 필요하다.
     */
    @Transactional
    public void recordFailure(String orderId, String userId, String reason) {
        PaymentFailedEvent failed = PaymentFailedEvent.of(orderId, userId, reason);
        outboxRepository.save(OutboxEvent.pending(
                paymentFailedTopic, orderId, "PAYMENT_FAILED", serialize(failed)));
    }

    private String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event", e);
        }
    }
}
