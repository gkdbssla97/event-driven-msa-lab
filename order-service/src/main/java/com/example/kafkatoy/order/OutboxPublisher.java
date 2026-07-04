package com.example.kafkatoy.order;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
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
    private final Counter publishedCounter;
    private final Counter publishFailedCounter;

    public OutboxPublisher(
            OutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${app.kafka.topics.order-created}") String orderCreatedTopic
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.orderCreatedTopic = orderCreatedTopic;
        this.publishedCounter = Counter.builder("outbox.published")
                .description("Outbox events successfully published to Kafka")
                .register(meterRegistry);
        this.publishFailedCounter = Counter.builder("outbox.publish.failed")
                .description("Outbox events that failed to publish")
                .register(meterRegistry);
        // PENDING 건수를 실시간 Gauge로 노출 — Prometheus가 스크랩할 때마다 DB를 조회한다
        Gauge.builder("outbox.pending", outboxRepository,
                        repo -> repo.countByStatus(OutboxStatus.PENDING))
                .description("Number of outbox events still pending publication")
                .register(meterRegistry);
    }

    @Scheduled(
            initialDelayString = "${app.outbox.initial-delay-ms:1000}",
            fixedDelayString = "${app.outbox.poll-interval-ms:1000}"
    )
    @SchedulerLock(name = "outbox-publisher", lockAtLeastFor = "PT1S", lockAtMostFor = "PT30S")
    public void publishPending() {
        // ShedLock을 통과한, 즉 "이번 폴링 주기에 락을 잡은" 인스턴스만 이 줄을 찍는다.
        // 여러 인스턴스가 동시에 호출해도 락을 못 잡은 쪽은 이 로그 자체가 안 남는다.
        log.info("Outbox polling started — lock acquired (thread={})", Thread.currentThread().getName());
        outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING).forEach(event -> {
            kafkaTemplate.send(orderCreatedTopic, event.getAggregateId(), event.getPayload())
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            publishFailedCounter.increment();
                            log.error("Failed to publish outbox event: id={}, error={}", event.getId(), ex.getMessage());
                            return;
                        }
                        event.markPublished();
                        outboxRepository.save(event);
                        publishedCounter.increment();
                        log.info("Published outbox event: id={}, type={}, aggregateId={}",
                                event.getId(), event.getEventType(), event.getAggregateId());
                    });
        });
    }
}
