package com.example.kafkatoy.inventory;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // 목적지를 <원본토픽>.DLQ로 명시한다. 기본 recoverer는 ".DLT"를 붙이는데, 그러면
        // order-service의 DLQ 복구 리스너(order-created.DLQ 구독)가 poison을 못 받는다.
        // payment-service와 같은 규칙(.DLQ)으로 통일해 시스템 전체 DLQ 이름을 일치시킨다.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Publishing to DLQ: topic={}, key={}, error={}",
                            record.topic(), record.key(), ex.getMessage());
                    return new TopicPartition(record.topic() + ".DLQ", record.partition());
                });
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    }

    @Bean
    public NewTopic orderCreatedDlqTopic(@Value("${app.kafka.topics.order-created-dlq}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}
