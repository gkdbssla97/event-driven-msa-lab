package com.example.kafkatoy.order;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * 결제/재고 서비스가 응답 없이 죽거나 이벤트가 유실되면 사가가 STARTED/RESERVED 상태로
 * 영원히 멈춘다. 이 스위퍼가 주기적으로 그런 "멈춘 사가"를 찾아 자동으로 취소·보상한다.
 * (실무의 reconciliation/timeout-handler 배치에 해당 — 사가/워크플로우 시스템의 필수 요소)
 *
 * OutboxPublisher와 동일한 @Scheduled + DB 폴링 패턴이며, 대상 테이블만 saga_state다.
 */
@Component
public class SagaTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

    // 아직 안 끝난(비종착) 사가만 대상. 종착 상태는 이미 정리됐으므로 건드리지 않는다.
    private static final List<SagaStatus> NON_TERMINAL = List.of(SagaStatus.STARTED, SagaStatus.RESERVED);

    private final SagaStateRepository sagaStateRepository;
    private final OrderService orderService;
    private final long timeoutMs;
    private final Counter sweptCounter;

    public SagaTimeoutSweeper(SagaStateRepository sagaStateRepository,
                              OrderService orderService,
                              MeterRegistry meterRegistry,
                              @Value("${app.saga.timeout-ms:300000}") long timeoutMs) {
        this.sagaStateRepository = sagaStateRepository;
        this.orderService = orderService;
        this.timeoutMs = timeoutMs;
        this.sweptCounter = Counter.builder("saga.swept")
                .description("Number of stuck sagas the sweeper timed out and compensated")
                .register(meterRegistry);
    }

    @Scheduled(
            initialDelayString = "${app.saga.sweep-interval-ms:60000}",
            fixedDelayString = "${app.saga.sweep-interval-ms:60000}"
    )
    public void sweepStuckSagas() {
        Instant threshold = Instant.now().minus(Duration.ofMillis(timeoutMs));
        List<SagaState> stuck = sagaStateRepository.findByStatusInAndUpdatedAtBefore(NON_TERMINAL, threshold);
        if (stuck.isEmpty()) {
            return;
        }
        log.info("Saga sweeper: found {} stuck saga(s) older than {}ms", stuck.size(), timeoutMs);
        for (SagaState saga : stuck) {
            try {
                // 사가별 독립 트랜잭션 — 하나가 실패해도 나머지는 계속 처리된다.
                if (orderService.timeout(saga.getSagaId())) {
                    sweptCounter.increment();
                    log.warn("Saga timed out and compensated: sagaId={}, lastStatus={}",
                            saga.getSagaId(), saga.getStatus());
                }
            } catch (Exception e) {
                log.error("Failed to time out saga: sagaId={}, error={}", saga.getSagaId(), e.getMessage());
            }
        }
    }
}
