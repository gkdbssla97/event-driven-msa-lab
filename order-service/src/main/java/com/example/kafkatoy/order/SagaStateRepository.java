package com.example.kafkatoy.order;

import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaStateRepository extends JpaRepository<SagaState, String> {

    long countByStatus(SagaStatus status);

    /** 비종착 상태(STARTED/RESERVED 등)인데 threshold 이전에 마지막으로 갱신된, 멈춰있는 사가들. */
    List<SagaState> findByStatusInAndUpdatedAtBefore(List<SagaStatus> statuses, Instant threshold);
}
