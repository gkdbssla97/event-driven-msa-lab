package com.example.kafkatoy.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkatoy.contracts.InventoryReservedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

/**
 * inventory-reserved 소비 → payment-completed 발행까지 실제 Kafka 왕복으로 검증하는 E2E 테스트.
 * Outbox 도입 후 발행은 OutboxPublisher가 비동기로 하므로, 이 테스트에서만 폴링 주기를 짧게
 * 줘서(200ms) 10초 대기 안에 실제로 발행되는 것까지 본다 — 다른 payment-service 테스트들이
 * 폴러를 꺼두는 것과 반대다.
 *
 * 폴러가 살아있는 동안 다른 테스트의 pending 행을 건드리지 않도록, 공유 MySqlTestContainer가
 * 아니라 전용 컨테이너로 격리한다(order-service의 SkipLockedOutboxPublisherTest와 같은 이유).
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                "app.outbox.poll-interval-ms=200"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"inventory-reserved", "payment-completed", "payment-failed", "order-created.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class PaymentServiceApplicationTests {

    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    static {
        MYSQL.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void contextLoads() {
    }

    @Test
    void consumesInventoryReservedAndPublishesPaymentCompleted() throws Exception {
        InventoryReservedEvent source = InventoryReservedEvent.of("order-1", "user-1", "product-A", 2);
        kafkaTemplate.send("inventory-reserved", source.orderId(), objectMapper.writeValueAsString(source));

        Consumer<String, PaymentCompletedEvent> consumer = createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "payment-completed");

        PaymentCompletedEvent event = KafkaTestUtils.getSingleRecord(
                consumer,
                "payment-completed",
                Duration.ofSeconds(10)
        ).value();

        assertThat(event.orderId()).isEqualTo("order-1");
        assertThat(event.userId()).isEqualTo("user-1");
        assertThat(event.eventType()).isEqualTo("PAYMENT_COMPLETED");
    }

    private Consumer<String, PaymentCompletedEvent> createConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("payment-service-test", "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<PaymentCompletedEvent> jsonDeserializer = new JsonDeserializer<>(PaymentCompletedEvent.class);
        jsonDeserializer.addTrustedPackages("com.example.kafkatoy.contracts");

        return new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), jsonDeserializer).createConsumer();
    }
}
