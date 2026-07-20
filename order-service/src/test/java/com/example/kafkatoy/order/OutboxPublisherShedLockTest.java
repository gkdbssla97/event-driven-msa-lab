package com.example.kafkatoy.order;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.MySQLContainer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * order-service가 replicas=N으로 스케일아웃돼도, ShedLock이 같은 DB 락 테이블을
 * 보고 있는 한 OutboxPublisher.publishPending()은 동시에 단 1개의 인스턴스에서만
 * 실제로 실행되어야 한다.
 *
 * 두 스레드를 "가상의 파드 2개"로 보고 같은 메서드를 동시에 호출해,
 * OutboxRepository 조회가 정확히 1번만 일어나는지 검증한다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = {
                // ShedLock 전략을 활성화 (기본값은 skip-locked)
                "app.outbox.strategy=shedlock",
                // 컨텍스트 기동 시 자동으로 한 번 도는 @Scheduled가 우리 테스트의 락 획득과
                // 경합하지 않도록 첫 실행과 다음 실행 모두 테스트 시간보다 한참 뒤로 늦춤
                "app.outbox.initial-delay-ms=600000",
                "app.outbox.poll-interval-ms=600000"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "payment-completed", "payment-failed", "inventory-failed", "inventory-reserved", "order-created.DLQ", "inventory-reserved.DLQ"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class OutboxPublisherShedLockTest {

    // 이 테스트는 MySqlTestContainer(다른 테스트가 공유하는 컨테이너)를 쓰지 않고 전용 컨테이너를
    // 둔다. OrderServiceApplicationTests의 백그라운드 @Scheduled OutboxPublisher가 같은 shedlock
    // 행을 두고 경합하면(둘 다 락을 못 잡아) 이 테스트의 "정확히 1회 실행" 검증이 흔들리기 때문이다.
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
    private ShedLockOutboxPublisher outboxPublisher;

    @MockitoSpyBean
    private OutboxRepository outboxRepository;

    @Test
    void concurrentPublishPending_onlyOneInstanceActuallyRuns() throws InterruptedException {
        // 컨텍스트 기동 시 자동으로 한 번 돌았을 수 있는 호출 기록을 리셋
        clearInvocations(outboxRepository);

        int virtualPods = 2; // "가상 파드 2개"가 동시에 같은 스케줄러 메서드를 호출하는 상황
        ExecutorService pool = Executors.newFixedThreadPool(virtualPods);
        CountDownLatch readyLatch = new CountDownLatch(virtualPods);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(virtualPods);

        for (int i = 0; i < virtualPods; i++) {
            pool.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    outboxPublisher.publishPending();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        startLatch.countDown();
        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(finished).as("두 호출 모두 타임아웃 전에 끝나야 함").isTrue();

        // ShedLock이 동작한다면, 락을 못 잡은 쪽은 메서드 본문(repository 조회)까지
        // 도달하지 못하고 조용히 스킵되어야 한다 → 조회는 정확히 1번만 일어남
        verify(outboxRepository, times(1)).findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);
    }
}
