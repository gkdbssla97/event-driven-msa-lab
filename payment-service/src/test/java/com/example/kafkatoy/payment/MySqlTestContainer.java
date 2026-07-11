package com.example.kafkatoy.payment;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;

/**
 * 통합 테스트용 MySQL을 Testcontainers로 띄운다.
 * 정적 블록에서 한 번만 시작해 JVM 수명 동안 재사용하고(싱글턴 컨테이너 패턴),
 * 종료 시 Ryuk 컨테이너가 정리한다.
 */
public abstract class MySqlTestContainer {

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
}
