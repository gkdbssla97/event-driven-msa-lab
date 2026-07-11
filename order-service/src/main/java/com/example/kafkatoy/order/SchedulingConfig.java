package com.example.kafkatoy.order;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Outbox 폴링 스케줄러를 활성화한다. 발행 대상 선점 전략(ShedLock / SKIP LOCKED)과
 * 무관하게 항상 필요하다.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
