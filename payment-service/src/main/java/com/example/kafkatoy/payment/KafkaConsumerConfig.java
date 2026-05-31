package com.example.kafkatoy.payment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    // 재시도 없이 즉시 DLQ로 — 재시도 로직은 OrderCreatedEventListener 내부에서 처리
    private static final long DLQ_BACKOFF_INTERVAL = 0L;
    private static final long DLQ_MAX_ATTEMPTS = 1L;

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Publishing to DLQ: topic={}, key={}, error={}",
                            record.topic(), record.key(), ex.getMessage());
                    return new org.apache.kafka.common.TopicPartition(
                            record.topic() + ".DLQ", record.partition()
                    );
                });

        return new DefaultErrorHandler(recoverer, new FixedBackOff(DLQ_BACKOFF_INTERVAL, DLQ_MAX_ATTEMPTS));
    }
}
