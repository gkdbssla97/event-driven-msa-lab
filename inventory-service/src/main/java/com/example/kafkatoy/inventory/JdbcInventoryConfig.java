package com.example.kafkatoy.inventory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

/**
 * app.inventory.store=jdbc일 때만 DataSource·JdbcTemplate·TransactionTemplate를
 * 직접 등록한다. (기본 Redis 모드에서는 DataSource 자동설정을 꺼둔 상태라
 * MySQL 없이도 서비스가 기동된다.)
 */
@Configuration
@ConditionalOnProperty(name = "app.inventory.store", havingValue = "jdbc")
@EnableConfigurationProperties(DataSourceProperties.class)
public class JdbcInventoryConfig {

    @Bean
    DataSource dataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().build();
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean
    TransactionTemplate transactionTemplate(DataSource dataSource) {
        return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }
}
