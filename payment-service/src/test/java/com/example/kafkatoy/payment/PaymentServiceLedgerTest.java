package com.example.kafkatoy.payment;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PaymentService.process()가 결제 성공 시 원장(LedgerEntry)을 정확히 한 번만 append하고,
 * 잔액이 저장된 컬럼이 아니라 SUM으로 파생되는지 검증한다.
 *
 * Kafka를 거치지 않고 서비스 메서드를 직접 호출한다 — order-service의
 * SagaStateLifecycleTest와 같은 방식. @EmbeddedKafka는 KafkaTemplate을 쓰는 다른 빈들이
 * 컨텍스트에 함께 뜨기 때문에 부트스트랩 설정을 채우기 위해서만 필요하다.
 */
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {"inventory-reserved", "payment-completed", "payment-failed", "order-created.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class PaymentServiceLedgerTest extends MySqlTestContainer {

    @Autowired
    private PaymentService paymentService;
    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Test
    void process_recordsLedgerEntry_withQuantityTimesUnitPrice() {
        String orderId = UUID.randomUUID().toString();
        InventoryReservedEvent event = InventoryReservedEvent.of(orderId, "user-1", "product-A", 3);

        paymentService.process(event);

        LedgerEntry entry = ledgerEntryRepository.findById(LedgerEntry.ledgerId(orderId, LedgerEntryType.PAID))
                .orElseThrow();
        assertThat(entry.getAmount()).isEqualTo(3L * 10000L);
        assertThat(entry.getType()).isEqualTo(LedgerEntryType.PAID);
        assertThat(ledgerEntryRepository.sumAmountByOrderId(orderId)).isEqualTo(3L * 10000L);
    }

    @Test
    void process_calledTwice_doesNotDoubleTheLedgerEntry() {
        String orderId = UUID.randomUUID().toString();
        InventoryReservedEvent event = InventoryReservedEvent.of(orderId, "user-1", "product-A", 2);

        paymentService.process(event);
        paymentService.process(event); // 재전달 시뮬레이션 — PaymentRecord 게이트에 걸려 여기까지도 안 옴

        assertThat(ledgerEntryRepository.sumAmountByOrderId(orderId))
                .as("중복 호출이어도 원장 금액은 두 배가 되면 안 된다")
                .isEqualTo(2L * 10000L);
    }
}
