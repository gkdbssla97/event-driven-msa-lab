package com.example.kafkatoy.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import com.example.kafkatoy.contracts.PaymentCompletedEvent;
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

@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-completed"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class PaymentServiceApplicationTests {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void contextLoads() {
    }

    @Test
    void consumesOrderCreatedAndPublishesPaymentCompleted() {
        OrderCreatedEvent source = OrderCreatedEvent.initial("order-1", "user-1");
        kafkaTemplate.send("order-created", source.orderId(), source);

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
