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
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {"order-created"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderServiceApplicationTests {

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
                                  \"userId\": \"user-1\"
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.userId").value("user-1"))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.orderId").isNotEmpty());

        Consumer<String, OrderCreatedEvent> consumer = createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "order-created");

        OrderCreatedEvent event = KafkaTestUtils.getSingleRecord(consumer, "order-created", Duration.ofSeconds(10)).value();

        assertThat(event.userId()).isEqualTo("user-1");
        assertThat(event.orderId()).isNotBlank();
        assertThat(event.eventType()).isEqualTo("ORDER_CREATED");
    }

    @Test
    void postOrdersRejectsBlankUserId() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "userId": ""
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
