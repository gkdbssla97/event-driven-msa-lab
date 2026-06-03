package com.example.kafkatoy.order;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String orderCreatedTopic;

    public OutboxPublisher(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:1000}")
    @SchedulerLock(name = "outbox-publisher", lockAtLeastFor = "PT1S", lockAtMostFor = "PT30S")
    public void publishPending() {
        outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING).forEach(event -> {
            kafkaTemplate.send(orderCreatedTopic, event.getAggregateId(), event.getPayload())
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish outbox event: id={}, error={}", event.getId(), ex.getMessage());
                            return;
                        }
                        event.markPublished();
                        outboxRepository.save(event);
                        log.info("Published outbox event: id={}, type={}, aggregateId={}",
                                event.getId(), event.getEventType(), event.getAggregateId());
                    });
        });
    }
}
