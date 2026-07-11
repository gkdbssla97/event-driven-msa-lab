package com.example.kafkatoy.inventory;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 재고 예약 내역을 Redis에 저장한다.
 * 보상 트랜잭션(payment-failed) 수신 시 orderId로 예약 정보를 조회하여 재고를 복원하는 데 사용된다.
 * key: reservation:{orderId}  value: "{productId}:{quantity}"
 * (멱등성 처리는 RedisInventoryStore.claim/markOutcome이 담당한다.)
 */
@Component
public class ReservationStore {

    private static final Duration TTL = Duration.ofHours(24);
    private final StringRedisTemplate redisTemplate;

    public ReservationStore(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void save(String orderId, String productId, int quantity) {
        redisTemplate.opsForValue().set(key(orderId), productId + ":" + quantity, TTL);
    }

    private String key(String orderId) {
        return "reservation:" + orderId;
    }
}
