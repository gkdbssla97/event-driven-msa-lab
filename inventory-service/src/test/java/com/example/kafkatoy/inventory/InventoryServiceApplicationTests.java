package com.example.kafkatoy.inventory;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 기본(redis) 모드에서 애플리케이션 컨텍스트가 MySQL 없이도 기동되는지 검증한다.
 * DataSourceAutoConfiguration을 제외했기 때문에 DataSource 없이 부팅되어야 하며,
 * 활성 InventoryStore는 RedisInventoryStore여야 한다.
 */
@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "inventory-reserved", "inventory-failed", "payment-failed"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@Testcontainers
class InventoryServiceApplicationTests {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private InventoryStore inventoryStore;

    @Test
    void contextLoadsWithRedisStore() {
        assertThat(inventoryStore).isInstanceOf(RedisInventoryStore.class);
    }
}
