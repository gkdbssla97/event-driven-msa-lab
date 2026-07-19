package com.example.kafkatoy.order;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Grafana에서 "지금 어느 단계에 사가가 몇 개나 있는지" 바로 보이게 하는 게이지.
 * status=STARTED/RESERVED 합이 곧 "아직 안 끝난(비종착) 사가 수"다.
 * Outbox의 outbox.pending 게이지와 같은 방식 — Prometheus가 스크랩할 때마다 DB를 조회한다.
 */
@Component
public class SagaMetrics {

    public SagaMetrics(SagaStateRepository repository, MeterRegistry meterRegistry) {
        for (SagaStatus status : SagaStatus.values()) {
            Gauge.builder("saga.count", repository, repo -> repo.countByStatus(status))
                    .tag("status", status.name())
                    .description("Number of sagas currently in this status")
                    .register(meterRegistry);
        }
    }
}
