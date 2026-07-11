package com.example.kafkatoy.order;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * ShedLock 분산 락은 app.outbox.strategy=shedlock 모드에서만 활성화된다.
 * (기본 skip-locked 모드는 SELECT FOR UPDATE SKIP LOCKED로 중복 발행을 막으므로
 * ShedLock 락 테이블/프로바이더가 필요 없다.)
 */
@Configuration
@ConditionalOnProperty(name = "app.outbox.strategy", havingValue = "shedlock")
@EnableSchedulerLock(defaultLockAtMostFor = "PT30S")
public class ShedLockConfig {

    @Bean
    public LockProvider lockProvider(DataSource dataSource) {
        return new JdbcTemplateLockProvider(dataSource);
    }
}
