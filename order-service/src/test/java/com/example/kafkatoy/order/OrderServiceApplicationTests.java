package com.example.kafkatoy.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.kafkatoy.contracts.OrderCreatedEvent;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.MySQLContainer;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-completed", "payment-failed", "inventory-failed", "inventory-reserved", "order-created.DLQ", "inventory-reserved.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class OrderServiceApplicationTests {

    // 이 테스트는 Outbox 발행 스케줄러를 켠 채로 order-created의 "단일 레코드"를 단정한다.
    // 공유 컨테이너를 쓰면 다른 테스트 클래스가 남긴 PENDING order-created 행까지 발행되어
    // 레코드가 여러 개가 되므로, 전용 컨테이너로 격리한다(활성 발행 테스트의 공통 패턴).
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
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void contextLoads() {
    }

    @Test
    void postOrdersPublishesOrderCreatedEvent() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "userId": "user-1",
                                  "productId": "product-A",
                                  "quantity": 2
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.userId").value("user-1"))
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.orderId").isNotEmpty());

        Consumer<String, OrderCreatedEvent> consumer = createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "order-created");

        OrderCreatedEvent event = KafkaTestUtils.getSingleRecord(consumer, "order-created", Duration.ofSeconds(10)).value();

        assertThat(event.userId()).isEqualTo("user-1");
        assertThat(event.productId()).isEqualTo("product-A");
        assertThat(event.quantity()).isEqualTo(2);
        assertThat(event.orderId()).isNotBlank();
        assertThat(event.eventType()).isEqualTo("ORDER_CREATED");
    }

    @Test
    void postOrdersRejectsBlankUserId() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "userId": "",
                                  "productId": "product-A",
                                  "quantity": 1
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    private Consumer<String, OrderCreatedEvent> createConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("order-service-test", "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JsonDeserializer<OrderCreatedEvent> jsonDeserializer = new JsonDeserializer<>(OrderCreatedEvent.class);
        jsonDeserializer.addTrustedPackages("com.example.kafkatoy.contracts");

        return new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), jsonDeserializer).createConsumer();
    }
}
