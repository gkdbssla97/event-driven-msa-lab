package com.example.kafkatoy.payment;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Outbox 폴링 스케줄러(OutboxPublisher)를 활성화한다.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
