package com.example.kafkatoy.inventory;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 재고 예약 내역을 Redis에 저장한다.
 * 보상 트랜잭션(payment-failed) 수신 시 orderId로 예약 정보를 조회하여 재고를 복원하는 데 사용된다.
 * key: reservation:{orderId}  value: "{productId}:{quantity}"
 *
 * 멱등성 키: inventory:idempotency:{orderId}
 * Outbox 중복 발행 등으로 같은 order-created 이벤트가 재전달되어도 한 번만 처리하도록 보장한다.
 * SETNX(SET if Not eXists)를 사용해 "첫 처리자"를 원자적으로 결정한다.
 */
@Component
public class ReservationStore {

    private static final Duration TTL = Duration.ofHours(24);
    private static final Duration IDEMPOTENCY_TTL = Duration.ofDays(7);
    private final StringRedisTemplate redisTemplate;

    public ReservationStore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void save(String orderId, String productId, int quantity) {
        redisTemplate.opsForValue().set(key(orderId), productId + ":" + quantity, TTL);
    }

    /**
     * orderId에 대한 처리 권한을 원자적으로 획득한다.
     * Redis SETNX로 구현되므로 동시 호출 시 정확히 하나의 호출만 true를 반환한다.
     *
     * @return true if this is the first time this orderId is being processed
     */
    public boolean claimProcessing(String orderId) {
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent("inventory:idempotency:" + orderId, "1", IDEMPOTENCY_TTL);
        return Boolean.TRUE.equals(isNew);
    }

    private String key(String orderId) {
        return "reservation:" + orderId;
    }

    public record Reservation(String productId, int quantity) {}
}
