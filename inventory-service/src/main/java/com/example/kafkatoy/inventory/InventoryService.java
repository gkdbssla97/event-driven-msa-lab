package com.example.kafkatoy.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    // KEYS[1] = inventory:{productId}
    // ARGV[1] = quantity to reserve
    // Returns remaining stock after decrement, or -1 if insufficient
    private static final RedisScript<Long> RESERVE_SCRIPT = RedisScript.of("""
            local stock = redis.call('GET', KEYS[1])
            if not stock or tonumber(stock) < tonumber(ARGV[1]) then
                return -1
            end
            return redis.call('DECRBY', KEYS[1], ARGV[1])
            """, Long.class);

    private static final RedisScript<Long> RELEASE_SCRIPT = RedisScript.of("""
            return redis.call('INCRBY', KEYS[1], ARGV[1])
            """, Long.class);

    private final StringRedisTemplate redisTemplate;
    private final ReservationStore reservationStore;
    private final long initialStock;

    public InventoryService(StringRedisTemplate redisTemplate,
                            ReservationStore reservationStore,
                            @Value("${app.inventory.initial-stock:100}") long initialStock) {
        this.redisTemplate = redisTemplate;
        this.reservationStore = reservationStore;
        this.initialStock = initialStock;
    }

    /**
     * 재고를 원자적으로 차감한다.
     * Lua 스크립트로 GET + DECRBY를 단일 명령으로 실행하여 race condition을 방지.
     *
     * @return true if reservation succeeded
     */
    public boolean reserve(String orderId, String productId, int quantity) {
        String key = stockKey(productId);
        ensureInitialized(key);

        Long remaining = redisTemplate.execute(RESERVE_SCRIPT, List.of(key), String.valueOf(quantity));
        if (remaining == null || remaining < 0) {
            log.warn("Insufficient stock: productId={}, requested={}", productId, quantity);
            return false;
        }
        reservationStore.save(orderId, productId, quantity);
        log.info("Stock reserved: productId={}, quantity={}, remaining={}", productId, quantity, remaining);
        return true;
    }

    /**
     * 결제 실패 시 보상 트랜잭션으로 재고를 복원한다.
     */
    public void release(String productId, int quantity) {
        String key = stockKey(productId);
        Long stock = redisTemplate.execute(RELEASE_SCRIPT, List.of(key), String.valueOf(quantity));
        log.info("Stock released (compensation): productId={}, quantity={}, current={}", productId, quantity, stock);
    }

    public long getStock(String productId) {
        String val = redisTemplate.opsForValue().get(stockKey(productId));
        return val == null ? 0 : Long.parseLong(val);
    }

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
