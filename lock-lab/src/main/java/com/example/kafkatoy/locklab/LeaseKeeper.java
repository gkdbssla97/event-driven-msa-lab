package com.example.kafkatoy.locklab;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 작업이 도는 동안 리스를 주기적으로 연장하고, 리스를 잃었는지 작업에 알려 준다.
 *
 * <p>두 가지 경우를 "잃음"으로 본다.
 * <ol>
 *   <li>갱신의 영향 행이 0: 다른 인스턴스가 이미 가져갔다.</li>
 *   <li>DB 오류 등으로 갱신이 계속 실패해, 마지막 성공 후 (TTL - 여유)가 지났다.
 *       DB의 만료 시각이 오기 전에 스스로 손을 뗀다.</li>
 * </ol>
 * 작업은 단계 사이마다 {@link #isLost()}를 확인하고, true면 더 쓰지 않고 멈춘다.
 */
public final class LeaseKeeper implements AutoCloseable {

    private final Connection connection;
    private final LeaseLock.Lease lease;
    private final Duration ttl;
    private final long safetyMarginNanos;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "lease-keeper");
                t.setDaemon(true);
                return t;
            });

    private volatile long lastRenewedAtNanos;
    private volatile boolean takenOver;

    /**
     * @param connection 갱신 전용 연결. 작업 연결과 나누는 것이 좋다
     * @param interval   갱신 주기. TTL의 1/3 이하로 둔다(두 번 실패해도 아직 만료 전)
     */
    public LeaseKeeper(Connection connection, LeaseLock.Lease lease, Duration ttl, Duration interval) {
        if (interval.compareTo(ttl.dividedBy(3)) > 0) {
            throw new IllegalArgumentException("갱신 주기는 TTL의 1/3 이하여야 합니다");
        }
        this.connection = connection;
        this.lease = lease;
        this.ttl = ttl;
        this.safetyMarginNanos = ttl.dividedBy(5).toNanos();
        // 획득 직후를 마지막 성공 시점으로 본다
        this.lastRenewedAtNanos = System.nanoTime();
        scheduler.scheduleAtFixedRate(this::renewOnce, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void renewOnce() {
        if (takenOver) {
            return;
        }
        try {
            if (LeaseLock.renew(connection, lease, ttl)) {
                lastRenewedAtNanos = System.nanoTime();
            } else {
                takenOver = true;
            }
        } catch (SQLException e) {
            // 실패를 여기서 판정하지 않는다. isLost()의 로컬 마감 시각이 판정한다.
        }
    }

    /** 리스를 잃었거나, 잃었을 수 있어 멈춰야 하는 상태면 true. */
    public boolean isLost() {
        return takenOver || System.nanoTime() - lastRenewedAtNanos > ttl.toNanos() - safetyMarginNanos;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
    }
}
