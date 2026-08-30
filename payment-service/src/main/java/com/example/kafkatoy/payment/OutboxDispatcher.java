package com.example.kafkatoy.payment;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Counter publishedCounter;
    private final Counter publishFailedCounter;

    public OutboxDispatcher(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            MeterRegistry meterRegistry
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.publishedCounter = Counter.builder("outbox.published")
                .description("Outbox events successfully published to Kafka")
                .register(meterRegistry);
        this.publishFailedCounter = Counter.builder("outbox.publish.failed")
                .description("Outbox events that failed to publish")
                .register(meterRegistry);
        Gauge.builder("outbox.pending", outboxRepository,
                        repo -> repo.countByStatus(OutboxStatus.PENDING))
                .description("Number of outbox events still pending publication")
                .register(meterRegistry);
    }

    public void dispatch(OutboxEvent event) {
        try {
            kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                    .get(10, TimeUnit.SECONDS);
            event.markPublished();
            outboxRepository.save(event);
            publishedCounter.increment();
            log.info("Published outbox event: id={}, type={}, aggregateId={}",
                    event.getId(), event.getEventType(), event.getAggregateId());
        } catch (Exception e) {
            publishFailedCounter.increment();
            log.error("Failed to publish outbox event: id={}, error={}", event.getId(), e.getMessage());
        }
    }
}
