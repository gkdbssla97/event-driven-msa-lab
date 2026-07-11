package com.example.kafkatoy.order;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 대안 Outbox 발행 전략: ShedLock 분산 락 (app.outbox.strategy=shedlock).
 *
 * 여러 파드가 동시에 폴링해도 DB 락 테이블을 통해 "이번 주기에 한 파드"만
 * publishPending()을 실행한다. 확실하지만 나머지 파드는 놀게 되어(직렬화)
 * 스케일아웃의 병렬 처리량을 살리지 못한다. SKIP LOCKED 전략과의 비교군.
 */
@Component
@ConditionalOnProperty(name = "app.outbox.strategy", havingValue = "shedlock")
public class ShedLockOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(ShedLockOutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final OutboxDispatcher dispatcher;

    public ShedLockOutboxPublisher(OutboxRepository outboxRepository, OutboxDispatcher dispatcher) {
        this.outboxRepository = outboxRepository;
        this.dispatcher = dispatcher;
    }

    @Scheduled(
            initialDelayString = "${app.outbox.initial-delay-ms:1000}",
            fixedDelayString = "${app.outbox.poll-interval-ms:1000}"
    )
    @SchedulerLock(name = "outbox-publisher", lockAtLeastFor = "PT1S", lockAtMostFor = "PT30S")
    public void publishPending() {
        // 락을 잡은 인스턴스만 이 줄에 도달한다.
        log.info("Outbox polling started — lock acquired (thread={})", Thread.currentThread().getName());
        outboxRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING).forEach(dispatcher::dispatch);
    }
}
