package com.example.kafkatoy.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

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

    // KEYS[1] = reservation:{orderId}
    // 예약 조회(GET) + 재고 복원(INCRBY) + 예약 삭제(DEL)를 단일 원자적 명령으로 실행.
    // INCRBY 성공 후 DEL 실패로 예약이 남는 경우를 방지 (재전달 시 중복 복원 방지).
    // 예약이 없으면(이미 처리됨) nil 반환.
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
     * 보상 트랜잭션을 원자적으로 수행한다 (예약 조회 + 재고 복원 + 예약 삭제).
     * 재고 복원과 예약 키 삭제가 단일 Redis 명령으로 실행되므로, 둘 중 하나만
     * 적용되는 부분 실패 상태가 발생할 수 없다 — 이벤트가 재전달되어도 예약이
     * 이미 삭제되어 있으면 재복원이 일어나지 않는다.
     *
     * @return 복원된 productId/quantity, 이미 처리되어 예약이 없으면 empty
     */
    @SuppressWarnings("unchecked")
    public Optional<ReservationStore.Reservation> compensate(String orderId) {
        String key = "reservation:" + orderId;
        List<String> result = redisTemplate.execute(COMPENSATE_SCRIPT, List.of(key));
        if (result == null) {
            return Optional.empty();
        }
        String productId = result.get(0);
        int quantity = Integer.parseInt(result.get(1));
        log.info("Compensation complete (atomic): orderId={}, productId={}, quantity={}", orderId, productId, quantity);
        return Optional.of(new ReservationStore.Reservation(productId, quantity));
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
