package com.example.kafkatoy.inventory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * Redis + Lua 기반 재고 저장소. GET+DECRBY(차감), GET+INCRBY+DEL(복원)을 각각
 * 단일 Lua 스크립트로 묶어 Redis의 단일 스레드 실행으로 원자성을 얻는다.
 *
 * app.inventory.store 미지정 또는 redis일 때 활성화된다(기본값).
 */
@Service
@ConditionalOnProperty(name = "app.inventory.store", havingValue = "redis", matchIfMissing = true)
public class RedisInventoryStore implements InventoryStore {

    private static final Logger log = LoggerFactory.getLogger(RedisInventoryStore.class);

    // KEYS[1] = inventory:{productId}, ARGV[1] = quantity
    // 남은 재고 반환, 부족하면 -1
    private static final RedisScript<Long> RESERVE_SCRIPT = RedisScript.of("""
            local stock = redis.call('GET', KEYS[1])
            if not stock or tonumber(stock) < tonumber(ARGV[1]) then
                return -1
            end
            return redis.call('DECRBY', KEYS[1], ARGV[1])
            """, Long.class);

    // KEYS[1] = reservation:{orderId}
    // 예약 조회 + 재고 복원(INCRBY) + 예약 삭제(DEL)를 단일 원자 명령으로 실행.
    // 예약이 없으면(이미 처리됨) nil 반환 → 중복 복원 방지.
    private static final RedisScript<List> COMPENSATE_SCRIPT = RedisScript.of("""
            local reservation = redis.call('GET', KEYS[1])
            if not reservation then
                return nil
            end
            local sep = string.find(reservation, ':')
            local productId = string.sub(reservation, 1, sep - 1)
            local quantity = string.sub(reservation, sep + 1)
            redis.call('INCRBY', 'inventory:' .. productId, tonumber(quantity))
            redis.call('DEL', KEYS[1])
            return {productId, quantity}
            """, List.class);

    private final StringRedisTemplate redisTemplate;
    private final ReservationStore reservationStore;
    private final long initialStock;

    private final Counter reserveSuccessCounter;
    private final Counter reserveFailureCounter;
    private final Counter compensateRestoredCounter;
    private final Counter compensateNoopCounter;

    public RedisInventoryStore(StringRedisTemplate redisTemplate,
                               ReservationStore reservationStore,
                               MeterRegistry meterRegistry,
                               @Value("${app.inventory.initial-stock:100}") long initialStock) {
        this.redisTemplate = redisTemplate;
        this.reservationStore = reservationStore;
        this.initialStock = initialStock;
        this.reserveSuccessCounter = Counter.builder("inventory.reserve")
                .tag("result", "success").tag("backend", "redis").register(meterRegistry);
        this.reserveFailureCounter = Counter.builder("inventory.reserve")
                .tag("result", "insufficient_stock").tag("backend", "redis").register(meterRegistry);
        this.compensateRestoredCounter = Counter.builder("inventory.compensate")
                .tag("result", "restored").tag("backend", "redis").register(meterRegistry);
        this.compensateNoopCounter = Counter.builder("inventory.compensate")
                .tag("result", "not_found").tag("backend", "redis").register(meterRegistry);
    }

    @Override
    public boolean reserve(String orderId, String productId, int quantity) {
        String key = stockKey(productId);
        ensureInitialized(key);

        Long remaining = redisTemplate.execute(RESERVE_SCRIPT, List.of(key), String.valueOf(quantity));
        if (remaining == null || remaining < 0) {
            log.warn("Insufficient stock: productId={}, requested={}", productId, quantity);
            reserveFailureCounter.increment();
            return false;
        }
        reservationStore.save(orderId, productId, quantity);
        reserveSuccessCounter.increment();
        log.info("Stock reserved: productId={}, quantity={}, remaining={}", productId, quantity, remaining);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<Reservation> compensate(String orderId) {
        String key = "reservation:" + orderId;
        List<String> result = redisTemplate.execute(COMPENSATE_SCRIPT, List.of(key));
        if (result == null) {
            compensateNoopCounter.increment();
            return Optional.empty();
        }
        String productId = result.get(0);
        int quantity = Integer.parseInt(result.get(1));
        compensateRestoredCounter.increment();
        log.info("Compensation complete (atomic): orderId={}, productId={}, quantity={}", orderId, productId, quantity);
        return Optional.of(new Reservation(productId, quantity));
    }

    @Override
    public boolean claimProcessing(String orderId) {
        return reservationStore.claimProcessing(orderId);
    }

    @Override
    public long getStock(String productId) {
        String val = redisTemplate.opsForValue().get(stockKey(productId));
        return val == null ? 0 : Long.parseLong(val);
    }

    @Override
    public void initStock(String productId, int stock) {
        redisTemplate.opsForValue().set(stockKey(productId), String.valueOf(stock));
        log.info("Stock initialized: productId={}, stock={}", productId, stock);
    }

    private void ensureInitialized(String key) {
        redisTemplate.opsForValue().setIfAbsent(key, String.valueOf(initialStock));
    }

    private String stockKey(String productId) {
        return "inventory:" + productId;
    }
}
