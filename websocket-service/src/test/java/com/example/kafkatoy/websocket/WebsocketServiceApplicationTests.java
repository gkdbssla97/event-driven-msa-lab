package com.example.kafkatoy.websocket;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@EmbeddedKafka(partitions = 1, topics = {"payment-completed"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class WebsocketServiceApplicationTests {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @MockBean
    private SimpMessagingTemplate simpMessagingTemplate;

    @Test
    void contextLoads() {
    }

    @Test
    void consumesPaymentCompletedAndRelaysToWebsocketDestination() {
        PaymentCompletedEvent event = PaymentCompletedEvent.initial("order-1", "user-1");

        kafkaTemplate.send("payment-completed", event.orderId(), event);

        verify(simpMessagingTemplate, timeout(10000)).convertAndSend(
                eq("/topic/payments/user-1"),
                eq(event)
        );
    }
}
