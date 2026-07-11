package com.example.kafkatoy.inventory;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * RDB 조건부 UPDATE(UPDATE ... WHERE stock >= ?)가 락 명령 없이도 동시 요청 환경에서
 * 오버셀(재고 음수)을 막는지, 그리고 보상이 멱등한지 검증한다.
 * Redis Lua 버전(InventoryServiceConcurrencyTest)과 같은 주장을 RDB로 재현한다.
 */
@Testcontainers
class JdbcInventoryStoreConcurrencyTest {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    private HikariDataSource dataSource;
    private JdbcInventoryStore store;

    @BeforeEach
    void setUp() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(MYSQL.getJdbcUrl());
        dataSource.setUsername(MYSQL.getUsername());
        dataSource.setPassword(MYSQL.getPassword());
        dataSource.setMaximumPoolSize(20);

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        store = new JdbcInventoryStore(jdbc, tx, new SimpleMeterRegistry(), 100L);
        store.createSchema();
    }

    @Test
    void concurrentReserve_neverOversells() throws InterruptedException {
        String productId = "jdbc-concurrency-" + System.nanoTime();
        int initialStock = 10;
        int concurrentRequests = 50;

        store.initStock(productId, initialStock);

        ExecutorService pool = Executors.newFixedThreadPool(concurrentRequests);
        CountDownLatch readyLatch = new CountDownLatch(concurrentRequests);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrentRequests);
        AtomicInteger successCount = new AtomicInteger();

        for (int i = 0; i < concurrentRequests; i++) {
            String orderId = "jdbc-order-" + System.nanoTime() + "-" + i;
            pool.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    if (store.reserve(orderId, productId, 1)) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        boolean finished = doneLatch.await(20, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(finished).as("모든 요청이 타임아웃 전에 끝나야 함").isTrue();
        assertThat(successCount.get())
                .as("재고(%d)보다 많은 요청(%d)이 와도 성공은 재고만큼만", initialStock, concurrentRequests)
                .isEqualTo(initialStock);
        assertThat(store.getStock(productId))
                .as("오버셀(재고 음수) 발생 금지")
                .isEqualTo(0);
    }

    @Test
    void claim_reportsPriorOutcomeForReplay() {
        String orderId = "jdbc-idem-" + System.nanoTime();

        // 최초 처리 권한 획득
        assertThat(store.claim(orderId)).isEqualTo(InventoryStore.ClaimResult.CLAIMED);
        // 결과 기록 전 재전달 → 재발행 근거 없음(첫 처리 진행 중/크래시)
        assertThat(store.claim(orderId)).isEqualTo(InventoryStore.ClaimResult.IN_PROGRESS);
        // 결과 기록 후 재전달 → 저장된 결과를 알려줘 replay 가능
        store.markOutcome(orderId, true);
        assertThat(store.claim(orderId)).isEqualTo(InventoryStore.ClaimResult.DUPLICATE_RESERVED);
    }

    @Test
    void compensate_isIdempotent() {
        String productId = "jdbc-compensate-" + System.nanoTime();
        String orderId = "jdbc-order-" + System.nanoTime();
        store.initStock(productId, 5);

        assertThat(store.reserve(orderId, productId, 3)).isTrue();
        assertThat(store.getStock(productId)).isEqualTo(2);

        // 첫 보상: 복원됨
        assertThat(store.compensate(orderId)).isPresent();
        assertThat(store.getStock(productId)).isEqualTo(5);

        // 재전달을 흉내낸 두 번째 보상: 예약이 이미 없으므로 복원하지 않음(멱등)
        assertThat(store.compensate(orderId)).isEmpty();
        assertThat(store.getStock(productId)).as("이중 복원 금지").isEqualTo(5);
    }
}
