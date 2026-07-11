package com.example.kafkatoy.inventory;

import java.util.Optional;

/**
 * 재고 저장소 추상화. 같은 "원자적 재고 차감/복원 + 멱등성" 계약을
 * 서로 다른 백엔드로 구현해 비교한다.
 *
 * - {@link RedisInventoryStore}: Redis + Lua 스크립트 (단일 스레드 원자성)
 * - {@link JdbcInventoryStore}: RDB + 조건부 UPDATE (WHERE 가드로 원자성)
 *
 * app.inventory.store 프로퍼티(redis|jdbc)로 활성 구현을 고른다.
 */
public interface InventoryStore {

    /** 재고를 원자적으로 차감하고 예약을 기록한다. 성공 시 true. */
    boolean reserve(String orderId, String productId, int quantity);

    /** 보상: 예약을 조회해 재고를 복원하고 예약을 삭제한다(멱등). 복원했으면 내역 반환. */
    Optional<Reservation> compensate(String orderId);

    /**
     * orderId에 대한 처리 권한을 원자적으로 획득한다.
     * 최초면 CLAIMED, 중복이면 저장된 이전 결과(RESERVED/FAILED)를 알려준다.
     * 첫 처리가 결과를 남기기 전에 죽은 경우는 IN_PROGRESS.
     */
    ClaimResult claim(String orderId);

    /** claim(CLAIMED) 후 실제 처리 결과를 기록한다. 중복 재전달 시 replay에 쓰인다. */
    void markOutcome(String orderId, boolean reserved);

    long getStock(String productId);

    void initStock(String productId, int stock);

    record Reservation(String productId, int quantity) {}

    /**
     * 멱등성 판정 결과.
     * - CLAIMED: 최초 처리 → reserve 진행 후 markOutcome
     * - DUPLICATE_RESERVED / DUPLICATE_FAILED: 이미 처리됨 → 같은 결과 이벤트를 재발행(replay)
     * - IN_PROGRESS: 첫 처리가 결과를 남기기 전(=크래시 등) → 재발행할 근거가 없어 스킵
     */
    enum ClaimResult { CLAIMED, DUPLICATE_RESERVED, DUPLICATE_FAILED, IN_PROGRESS }
}
