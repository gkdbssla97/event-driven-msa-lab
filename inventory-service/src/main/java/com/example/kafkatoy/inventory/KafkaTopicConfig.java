package com.example.kafkatoy.inventory;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic inventoryReservedTopic(@Value("${app.kafka.topics.inventory-reserved}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryFailedTopic(@Value("${app.kafka.topics.inventory-failed}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
