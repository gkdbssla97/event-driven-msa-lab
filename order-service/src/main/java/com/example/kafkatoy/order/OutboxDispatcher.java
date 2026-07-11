package com.example.kafkatoy.order;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Outbox 이벤트를 실제로 Kafka에 발행하고 상태/메트릭을 갱신한다.
 * 어떤 방식으로 발행 대상을 "선점"하느냐(ShedLock vs SKIP LOCKED)와 무관하게
 * 공통으로 쓰이는 발행 로직을 모은다.
 *
 * 발행은 동기(ack 대기)로 한다: 전송이 성공한 뒤에만 PUBLISHED로 마킹하므로,
 * SKIP LOCKED 전략에서 트랜잭션 커밋(=행 락 해제) 시점이 전송 성공과 정합된다.
 */
@Component
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String orderCreatedTopic;
    private final Counter publishedCounter;
    private final Counter publishFailedCounter;

    public OutboxDispatcher(
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
        Gauge.builder("outbox.pending", outboxRepository,
                        repo -> repo.countByStatus(OutboxStatus.PENDING))
                .description("Number of outbox events still pending publication")
                .register(meterRegistry);
    }

    /**
     * 이벤트 하나를 동기 발행하고 PUBLISHED로 마킹한다. 전송 실패 시 마킹하지 않아
     * 다음 폴링에서 재시도된다(at-least-once). 실패해도 예외를 던지지 않으므로,
     * 배치 처리 중 한 건이 실패해도 나머지 건은 계속 발행된다.
     */
    public void dispatch(OutboxEvent event) {
        try {
            kafkaTemplate.send(orderCreatedTopic, event.getAggregateId(), event.getPayload())
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
