package com.example.kafkatoy.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 기본 Outbox 발행 전략: SELECT ... FOR UPDATE SKIP LOCKED.
 *
 * ShedLock처럼 "한 파드만 실행"으로 직렬화하지 않고, 모든 파드가 동시에 폴링하되
 * 행 락으로 서로 다른 배치를 나눠 가진다. 스케일아웃 시 병렬 처리량을 살리면서도
 * 같은 이벤트가 두 번 발행되지 않는다.
 *
 * 트랜잭션 안에서 배치를 잠그고 발행 → 커밋 시 락 해제. 발행은 동기(ack 대기)라
 * 전송 성공 후에만 PUBLISHED로 마킹된다.
 */
@Component
@ConditionalOnProperty(name = "app.outbox.strategy", havingValue = "skip-locked", matchIfMissing = true)
public class SkipLockedOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(SkipLockedOutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final OutboxDispatcher dispatcher;
    private final int batchSize;

    public SkipLockedOutboxPublisher(
            OutboxRepository outboxRepository,
            OutboxDispatcher dispatcher,
            @org.springframework.beans.factory.annotation.Value("${app.outbox.batch-size:100}") int batchSize
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
