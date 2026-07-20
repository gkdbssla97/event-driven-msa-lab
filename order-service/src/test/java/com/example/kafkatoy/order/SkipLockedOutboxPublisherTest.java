package com.example.kafkatoy.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.MySQLContainer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SELECT ... FOR UPDATE SKIP LOCKED로 폴링하면, 여러 인스턴스가 동시에 폴링해도
 * 서로가 잠근 행을 건너뛰고 겹치지 않는(disjoint) 배치를 나눠 가진다는 것을 검증한다.
 *
 * "가상 파드 2개"를 각자 별도 트랜잭션으로 두고, 첫 번째가 한 행을 잠근 상태에서
 * 두 번째가 폴링하면 잠긴 행을 건너뛰고 다른 행을 가져오는지 확인한다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                // 백그라운드 스케줄러가 테스트용 PENDING 행을 먼저 발행하지 않도록 지연을 크게
                "app.outbox.initial-delay-ms=600000",
                "app.outbox.poll-interval-ms=600000"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-completed", "payment-failed", "inventory-failed", "inventory-reserved", "order-created.DLQ", "inventory-reserved.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class SkipLockedOutboxPublisherTest {

    // 다른 테스트의 활성 스케줄러가 이 테스트의 PENDING 행을 발행해버리지 않도록 전용 컨테이너로 격리.
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    static {
        MYSQL.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
    }

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private PlatformTransactionManager txManager;

    @Test
    void concurrentPollers_grabDisjointRows() throws InterruptedException {
        TransactionTemplate seed = new TransactionTemplate(txManager);
        seed.executeWithoutResult(s -> {
            outboxRepository.save(OutboxEvent.pending("order-created", "agg-1", "ORDER_CREATED", "{}"));
            outboxRepository.save(OutboxEvent.pending("order-created", "agg-2", "ORDER_CREATED", "{}"));
        });

        CountDownLatch aLocked = new CountDownLatch(1);
        CountDownLatch bDone = new CountDownLatch(1);
        AtomicReference<String> aId = new AtomicReference<>();
        AtomicReference<String> bId = new AtomicReference<>();

        Thread a = new Thread(() -> new TransactionTemplate(txManager).executeWithoutResult(s -> {
            List<OutboxEvent> batch = outboxRepository.lockPendingBatch(1);
            aId.set(batch.isEmpty() ? null : batch.get(0).getId());
            aLocked.countDown();
            // 락을 쥔 채로 B가 폴링을 마칠 때까지 대기 → B는 이 행을 SKIP 해야 함
            await(bDone);
        }));

        Thread b = new Thread(() -> {
            await(aLocked); // A가 한 행을 잠글 때까지 대기
            new TransactionTemplate(txManager).executeWithoutResult(s -> {
                List<OutboxEvent> batch = outboxRepository.lockPendingBatch(1);
                bId.set(batch.isEmpty() ? null : batch.get(0).getId());
            });
            bDone.countDown();
        });

        a.start();
        b.start();
        a.join(15000);
        b.join(15000);

        assertThat(aId.get()).as("첫 폴러는 한 행을 잠근다").isNotNull();
        assertThat(bId.get())
                .as("SKIP LOCKED: 두 번째 폴러는 잠긴 행을 건너뛰고 다른 행을 가져와야 한다")
                .isNotNull();
        assertThat(aId.get())
                .as("두 폴러가 서로 다른(disjoint) 행을 나눠 가진다")
                .isNotEqualTo(bId.get());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
