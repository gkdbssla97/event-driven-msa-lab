package com.example.kafkatoy.inventory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * RDB(MySQL) 기반 재고 저장소. Redis Lua의 대응물로, "락 명령 없이" 조건부 UPDATE의
 * WHERE 가드로 원자적 차감을 구현한다.
 *
 *   UPDATE inventory SET stock = stock - ? WHERE product_id = ? AND stock >= ?
 *
 * 이 한 문장이 InnoDB 행 락으로 직렬화되므로, JPA 엔티티를 읽어서 값을 고쳐 저장하는
 * read-modify-write(=lost update 위험)와 달리 동시 요청에도 오버셀이 나지 않는다.
 *
 * app.inventory.store=jdbc일 때만 활성화된다.
 */
@Service
@ConditionalOnProperty(name = "app.inventory.store", havingValue = "jdbc")
public class JdbcInventoryStore implements InventoryStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcInventoryStore.class);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final long initialStock;

    private final Counter reserveSuccessCounter;
    private final Counter reserveFailureCounter;
    private final Counter compensateRestoredCounter;
    private final Counter compensateNoopCounter;

    public JdbcInventoryStore(JdbcTemplate jdbc,
                              TransactionTemplate tx,
                              MeterRegistry meterRegistry,
                              @Value("${app.inventory.initial-stock:100}") long initialStock) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.initialStock = initialStock;
        this.reserveSuccessCounter = Counter.builder("inventory.reserve")
                .tag("result", "success").tag("backend", "jdbc").register(meterRegistry);
        this.reserveFailureCounter = Counter.builder("inventory.reserve")
                .tag("result", "insufficient_stock").tag("backend", "jdbc").register(meterRegistry);
        this.compensateRestoredCounter = Counter.builder("inventory.compensate")
                .tag("result", "restored").tag("backend", "jdbc").register(meterRegistry);
        this.compensateNoopCounter = Counter.builder("inventory.compensate")
                .tag("result", "not_found").tag("backend", "jdbc").register(meterRegistry);
    }

    @PostConstruct
    void createSchema() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS inventory (
                    product_id VARCHAR(255) NOT NULL PRIMARY KEY,
                    stock      BIGINT       NOT NULL
                )""");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS reservations (
                    order_id   VARCHAR(255) NOT NULL PRIMARY KEY,
                    product_id VARCHAR(255) NOT NULL,
                    quantity   INT          NOT NULL
                )""");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS processed_orders (
                    order_id VARCHAR(255) NOT NULL PRIMARY KEY,
                    status   VARCHAR(20)  NOT NULL
                )""");
    }

    @Override
    public boolean reserve(String orderId, String productId, int quantity) {
        ensureInitialized(productId);

        Boolean ok = tx.execute(status -> {
            // 조건부 UPDATE: 재고가 충분할 때만 감소. 영향받은 행이 0이면 재고 부족.
            int updated = jdbc.update(
                    "UPDATE inventory SET stock = stock - ? WHERE product_id = ? AND stock >= ?",
                    quantity, productId, quantity);
            if (updated == 0) {
                return false;
            }
            // 보상에 대비해 예약 내역 기록 (차감과 같은 트랜잭션)
            jdbc.update("INSERT INTO reservations (order_id, product_id, quantity) VALUES (?, ?, ?)",
                    orderId, productId, quantity);
            return true;
        });

        if (Boolean.TRUE.equals(ok)) {
            reserveSuccessCounter.increment();
            log.info("Stock reserved (jdbc): productId={}, quantity={}", productId, quantity);
            return true;
        }
        reserveFailureCounter.increment();
        log.warn("Insufficient stock (jdbc): productId={}, requested={}", productId, quantity);
        return false;
    }

    @Override
    public Optional<Reservation> compensate(String orderId) {
        Reservation restored = tx.execute(status -> {
            // 예약 행을 잠근 채로 읽는다(FOR UPDATE) → 동시/재전달 보상이 직렬화되어
            // 두 번째 호출은 이미 삭제된 행을 못 보고 복원을 건너뛴다(멱등).
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT product_id, quantity FROM reservations WHERE order_id = ? FOR UPDATE", orderId);
            if (rows.isEmpty()) {
                return null;
            }
            String productId = (String) rows.get(0).get("product_id");
            int quantity = ((Number) rows.get(0).get("quantity")).intValue();
            jdbc.update("UPDATE inventory SET stock = stock + ? WHERE product_id = ?", quantity, productId);
            jdbc.update("DELETE FROM reservations WHERE order_id = ?", orderId);
            return new Reservation(productId, quantity);
        });

        if (restored == null) {
            compensateNoopCounter.increment();
            return Optional.empty();
        }
        compensateRestoredCounter.increment();
        log.info("Compensation complete (jdbc): orderId={}, productId={}, quantity={}",
                orderId, restored.productId(), restored.quantity());
        return Optional.of(restored);
    }

    @Override
    public ClaimResult claim(String orderId) {
        try {
            // order_id PK의 유니크 제약이 최초 1회만 삽입을 허용한다(원자적 멱등성 가드).
            jdbc.update("INSERT INTO processed_orders (order_id, status) VALUES (?, 'IN_PROGRESS')", orderId);
            return ClaimResult.CLAIMED;
        } catch (DuplicateKeyException e) {
            String status = jdbc.query(
                    "SELECT status FROM processed_orders WHERE order_id = ?",
                    rs -> rs.next() ? rs.getString(1) : null, orderId);
            if ("RESERVED".equals(status)) return ClaimResult.DUPLICATE_RESERVED;
            if ("FAILED".equals(status)) return ClaimResult.DUPLICATE_FAILED;
            return ClaimResult.IN_PROGRESS;
        }
    }

    @Override
    public void markOutcome(String orderId, boolean reserved) {
        jdbc.update("UPDATE processed_orders SET status = ? WHERE order_id = ?",
                reserved ? "RESERVED" : "FAILED", orderId);
    }

    @Override
    public long getStock(String productId) {
        Long stock = jdbc.query(
                "SELECT stock FROM inventory WHERE product_id = ?",
                rs -> rs.next() ? rs.getLong(1) : null,
                productId);
        return stock == null ? 0 : stock;
    }

    @Override
    public void initStock(String productId, int stock) {
        jdbc.update("""
                INSERT INTO inventory (product_id, stock) VALUES (?, ?)
                ON DUPLICATE KEY UPDATE stock = VALUES(stock)""", productId, stock);
        log.info("Stock initialized (jdbc): productId={}, stock={}", productId, stock);
    }

    private void ensureInitialized(String productId) {
        // 행이 없으면 초기 재고로 생성. 이미 있으면 무시.
        jdbc.update("INSERT IGNORE INTO inventory (product_id, stock) VALUES (?, ?)", productId, initialStock);
    }
}
