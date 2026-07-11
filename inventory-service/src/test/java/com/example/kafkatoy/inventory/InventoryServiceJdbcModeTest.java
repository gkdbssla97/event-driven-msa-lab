package com.example.kafkatoy.inventory;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * app.inventory.store=jdbc 모드에서 컨텍스트가 기동되고, JdbcInventoryConfig가
 * DataSource를 직접 제공해 활성 InventoryStore가 JdbcInventoryStore로 바뀌는지 검증한다.
 */
@SpringBootTest(
        webEnvironment = WebEnvironment.NONE,
        properties = "app.inventory.store=jdbc"
)
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-created", "inventory-reserved", "inventory-failed", "payment-failed"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@Testcontainers
class InventoryServiceJdbcModeTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
    }

    @Autowired
    private InventoryStore inventoryStore;

    @Test
    void contextLoadsWithJdbcStore() {
        assertThat(inventoryStore).isInstanceOf(JdbcInventoryStore.class);
    }
}
