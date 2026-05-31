package com.example.kafkatoy.order;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 토픽을 앱 코드에서 선언적으로 관리.
 * 파티션 3개: replicas가 3일 때 pod당 1파티션 담당 → 병렬 처리 보장.
 * 토픽이 이미 존재하면 idempotent하게 무시됨.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderCreatedTopic(@Value("${app.kafka.topics.order-created}") String topicName) {
        return TopicBuilder.name(topicName).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentCompletedTopic(@Value("${app.kafka.topics.payment-completed}") String topicName) {
        return TopicBuilder.name(topicName).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentFailedTopic(@Value("${app.kafka.topics.payment-failed}") String topicName) {
        return TopicBuilder.name(topicName).partitions(3).replicas(1).build();
    }
}
