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

    /** orderId에 대한 최초 처리 권한을 원자적으로 획득한다(멱등성 가드). 최초면 true. */
    boolean claimProcessing(String orderId);

    long getStock(String productId);

    void initStock(String productId, int stock);

    record Reservation(String productId, int quantity) {}
}
