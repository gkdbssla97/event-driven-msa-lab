package com.example.kafkatoy.inventory;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
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
 * Redis Lua vs RDB(조건부 UPDATE) 재고 차감의 처리량을 같은 워크로드로 비교한다.
 * 하나의 인기 상품(단일 행)에 다수 스레드가 동시에 차감을 시도하는, 오버셀이 문제되는
 * 실제 시나리오를 모사한다. 정확성은 assert하고 처리량(ops/sec)은 로그로 남긴다
 * (타이밍은 머신에 따라 달라 CI 게이트로 삼지 않는다).
 *
 * 규모는 시스템 프로퍼티로 조절: -Dbenchmark.ops=20000 -Dbenchmark.threads=50
 */
@Testcontainers
class InventoryStoreBenchmarkTest {

    private static final Logger log = LoggerFactory.getLogger(InventoryStoreBenchmarkTest.class);

    private static final int OPS = Integer.getInteger("benchmark.ops", 2000);
    private static final int THREADS = Integer.getInteger("benchmark.threads", 50);
    private static final int WARMUP = 200;

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    @Test
    void compareRedisVsRdbThroughput() throws InterruptedException {
        InventoryStore redis = buildRedisStore();
        InventoryStore jdbc = buildJdbcStore();

        // 워밍업(JIT/커넥션 풀 예열)
        runWorkload(redis, "warmup-redis", WARMUP, THREADS);
        runWorkload(jdbc, "warmup-jdbc", WARMUP, THREADS);

        Result redisResult = runWorkload(redis, "redis-" + System.nanoTime(), OPS, THREADS);
        Result jdbcResult = runWorkload(jdbc, "jdbc-" + System.nanoTime(), OPS, THREADS);

        // 정확성: 모든 차감이 성공하고 재고가 정확히 0
        assertThat(redisResult.success).isEqualTo(OPS);
        assertThat(jdbcResult.success).isEqualTo(OPS);

        double redisOps = OPS * 1000.0 / redisResult.elapsedMs;
        double jdbcOps = OPS * 1000.0 / jdbcResult.elapsedMs;

        log.info("""

                ===== 재고 차감 처리량 비교 (ops={}, threads={}, 단일 상품 경합) =====
                Redis Lua   : {} ms  ({} ops/sec)
                RDB  UPDATE : {} ms  ({} ops/sec)
                Redis가 RDB 대비 약 {}배 빠름
                ==============================================================""",
                OPS, THREADS,
                redisResult.elapsedMs, String.format("%.0f", redisOps),
                jdbcResult.elapsedMs, String.format("%.0f", jdbcOps),
                String.format("%.1f", redisOps / jdbcOps));
    }

    private Result runWorkload(InventoryStore store, String product, int ops, int threads) throws InterruptedException {
        store.initStock(product, ops); // 재고를 딱 ops만큼 → 전부 성공

        // 처리량 측정: ops개 작업을 threads개 워커 풀에 흘려 풀을 포화시킨 뒤 완료까지의
        // 벽시계 시간을 잰다. (ops ≫ threads이므로 "동시 발사" 배리어는 쓰지 않는다.)
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(ops);
        AtomicInteger success = new AtomicInteger();

        long t0 = System.nanoTime();
        for (int i = 0; i < ops; i++) {
            String orderId = product + "-order-" + i;
            pool.submit(() -> {
                try {
                    if (store.reserve(orderId, product, 1)) {
                        success.incrementAndGet();
                    }
                } finally {
                    done.countDown();
                }
            });
        }

        done.await(2, TimeUnit.MINUTES);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        pool.shutdownNow();

        return new Result(success.get(), elapsedMs);
    }

    private InventoryStore buildRedisStore() {
        LettuceConnectionFactory cf = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        cf.afterPropertiesSet();
        StringRedisTemplate redisTemplate = new StringRedisTemplate(cf);
        redisTemplate.afterPropertiesSet();
        return new RedisInventoryStore(redisTemplate, new ReservationStore(redisTemplate), new SimpleMeterRegistry(), 100L);
    }

    private InventoryStore buildJdbcStore() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(MYSQL.getJdbcUrl());
        ds.setUsername(MYSQL.getUsername());
        ds.setPassword(MYSQL.getPassword());
        ds.setMaximumPoolSize(THREADS);
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        JdbcInventoryStore store = new JdbcInventoryStore(jdbc, tx, new SimpleMeterRegistry(), 100L);
        store.createSchema();
        return store;
    }

    private record Result(int success, long elapsedMs) {}
}
