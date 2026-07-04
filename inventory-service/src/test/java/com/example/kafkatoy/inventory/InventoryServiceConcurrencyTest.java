package com.example.kafkatoy.inventory;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lua 스크립트(RESERVE_SCRIPT)가 GET+DECRBY를 원자적으로 묶어
 * 동시 요청 환경에서도 오버셀(재고 음수)이 발생하지 않음을 검증한다.
 *
 * 실행 전 로컬에 Redis가 떠 있어야 한다:
 *   docker run -d --name redis-test -p 6380:6379 redis:7-alpine
 */
class InventoryServiceConcurrencyTest {

    private static final int REDIS_PORT = 6380;

    private LettuceConnectionFactory connectionFactory;
    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory("localhost", REDIS_PORT);
        connectionFactory.afterPropertiesSet();

        StringRedisTemplate redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();

        ReservationStore reservationStore = new ReservationStore(redisTemplate);
        inventoryService = new InventoryService(redisTemplate, reservationStore, new SimpleMeterRegistry(), 100L);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void concurrentReserve_neverOversells() throws InterruptedException {
        String productId = "concurrency-test-" + System.nanoTime();
        int initialStock = 10;
        int concurrentRequests = 50;

        inventoryService.initStock(productId, initialStock);

        ExecutorService pool = Executors.newFixedThreadPool(concurrentRequests);
        CountDownLatch readyLatch = new CountDownLatch(concurrentRequests);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(concurrentRequests);
        AtomicInteger successCount = new AtomicInteger();

        for (int i = 0; i < concurrentRequests; i++) {
            String orderId = "order-" + i;
            pool.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    if (inventoryService.reserve(orderId, productId, 1)) {
                        successCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();      // 모든 스레드가 동시에 출발하도록 정렬
        startLatch.countDown();  // 동시 발사
        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(finished).as("모든 요청이 타임아웃 전에 끝나야 함").isTrue();
        assertThat(successCount.get())
                .as("재고(%d)보다 많은 요청(%d)이 와도 성공 건수는 재고만큼만 허용되어야 함", initialStock, concurrentRequests)
                .isEqualTo(initialStock);
        assertThat(inventoryService.getStock(productId))
                .as("오버셀(재고 음수) 발생 금지")
                .isEqualTo(0);
    }
}
