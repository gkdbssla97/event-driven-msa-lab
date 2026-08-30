package com.example.kafkatoy.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * order-service의 SkipLockedOutboxPublisher와 동일한 패턴. payment-service는 아직
 * 다중 인스턴스 스케일아웃 서사가 없어(ShedLock 의존성도 없음) 대안 전략 스위치 없이
 * 단일 구현만 둔다 — SKIP LOCKED 자체는 나중에 스케일아웃해도 안전하다.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final OutboxDispatcher dispatcher;
    private final int batchSize;

    public OutboxPublisher(
            OutboxRepository outboxRepository,
            OutboxDispatcher dispatcher,
            @Value("${app.outbox.batch-size:100}") int batchSize
    ) {
        this.outboxRepository = outboxRepository;
        this.dispatcher = dispatcher;
        this.batchSize = batchSize;
    }

    @Scheduled(
            initialDelayString = "${app.outbox.initial-delay-ms:1000}",
            fixedDelayString = "${app.outbox.poll-interval-ms:1000}"
    )
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxRepository.lockPendingBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }
        log.info("Outbox polling: locked {} pending events via SKIP LOCKED (thread={})",
                batch.size(), Thread.currentThread().getName());
        batch.forEach(dispatcher::dispatch);
    }
}
