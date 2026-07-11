package com.example.kafkatoy.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

// 기본 저장소는 Redis라, DataSource 자동설정을 꺼서 MySQL 없이도 기동되게 한다.
// jdbc 모드(app.inventory.store=jdbc)에서는 JdbcInventoryConfig가 DataSource를 직접 제공한다.
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class InventoryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
