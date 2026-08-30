package com.example.kafkatoy.payment;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxStatus status);

    long countByStatus(OutboxStatus status);

    /**
     * PENDING 이벤트 한 배치를 행 락으로 선점한다. FOR UPDATE로 선택한 행을 잠그되,
     * SKIP LOCKED로 다른 인스턴스가 이미 잠근 행은 건너뛴다. 여러 파드가 동시에
     * 폴링해도 서로 겹치지 않는 행을 나눠 가져 중복 발행 없이 병렬 처리된다.
     * 반드시 트랜잭션 안에서 호출해야 하며, 락은 커밋 시 해제된다.
     */
    @Query(value = "SELECT * FROM outbox_events WHERE status = 'PENDING' "
            + "ORDER BY created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> lockPendingBatch(@Param("limit") int limit);
}
