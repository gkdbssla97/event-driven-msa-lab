package com.example.kafkatoy.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * 사가(주문 하나) 전체의 진행 상태를 order/inventory/payment 세 서비스에 흩어진
 * 로컬 상태와 별개로 한 곳에 기록한다.
 *
 * Order.status(PENDING/CONFIRMED/CANCELED)는 order-service 자신의 최종 상태만
 * 말해줄 뿐, "재고는 이미 예약됐는데 결제 응답이 없다"처럼 사가가 정확히 어느
 * 단계에 멈췄는지는 알려주지 못한다. SagaState가 그 중간 지점을 기록한다.
 */
@Entity
@Table(name = "saga_state")
public class SagaState {

    @Id
    private String sagaId; // = orderId

    @Enumerated(EnumType.STRING)
    private SagaStatus status;

    private Instant createdAt;
    private Instant updatedAt;

    protected SagaState() {}

    public static SagaState started(String sagaId) {
        SagaState state = new SagaState();
        state.sagaId = sagaId;
        state.status = SagaStatus.STARTED;
        state.createdAt = Instant.now();
        state.updatedAt = state.createdAt;
        return state;
    }

    /**
     * 다음 단계로 전이한다. 이미 종착 상태(terminal)라면 무시한다 — 재전달나 순서
     * 뒤바뀜으로 종착 이후에 도착한 이벤트가 상태를 다시 되돌리지 못하게 막는다.
     */
    public void transitionTo(SagaStatus next) {
        if (status.isTerminal()) {
            return;
        }
        this.status = next;
        this.updatedAt = Instant.now();
    }

    public String getSagaId() { return sagaId; }
    public SagaStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
