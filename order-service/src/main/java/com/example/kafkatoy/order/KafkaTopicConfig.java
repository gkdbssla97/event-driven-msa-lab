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

    @Bean
    public NewTopic inventoryReservedTopic(@Value("${app.kafka.topics.inventory-reserved}") String topicName) {
        return TopicBuilder.name(topicName).partitions(3).replicas(1).build();
    }

    // DLQ 토픽은 격리하는 쪽(inventory/payment)의 recoverer가 발행 시 만들 수도 있지만,
    // 구독자인 order-service가 뜰 때 선언해 두면 브로커 auto-create 설정과 무관하게 존재가 보장된다.
    @Bean
    public NewTopic orderCreatedDlqTopic(@Value("${app.kafka.topics.order-created-dlq}") String topicName) {
        return TopicBuilder.name(topicName).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservedDlqTopic(@Value("${app.kafka.topics.inventory-reserved-dlq}") String topicName) {
        return TopicBuilder.name(topicName).partitions(3).replicas(1).build();
    }
}
