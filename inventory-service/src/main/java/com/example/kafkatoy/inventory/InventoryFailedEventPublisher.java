package com.example.kafkatoy.inventory;

import com.example.kafkatoy.contracts.InventoryFailedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryFailedEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryFailedEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public InventoryFailedEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                         ObjectMapper objectMapper,
                                         @Value("${app.kafka.topics.inventory-failed}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(InventoryFailedEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topic, event.orderId(), payload);
            log.info("Published inventory-failed: orderId={}, reason={}", event.orderId(), event.failureReason());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize InventoryFailedEvent", e);
        }
    }
}
