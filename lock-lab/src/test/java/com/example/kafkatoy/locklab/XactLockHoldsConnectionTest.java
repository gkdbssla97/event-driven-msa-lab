package com.example.kafkatoy.locklab;

import static com.example.kafkatoy.locklab.PgBouncerLab.log;
import static com.example.kafkatoy.locklab.PgBouncerLab.queryBoolean;
import static com.example.kafkatoy.locklab.PgBouncerLab.viaBouncer;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 실험 2. 트랜잭션 단위 advisory lock(pg_advisory_xact_lock)은 PgBouncer transaction 모드에서도 정확하다.
 * 대신 잠금을 유지하려면 작업 전체를 트랜잭션 하나로 묶어야 하고, 그동안 서버 연결 하나를 점유한다.
 */
class XactLockHoldsConnectionTest {

    private static final Duration JOB = Duration.ofSeconds(2);

    @BeforeEach
    @AfterEach
    void cleanUp() throws Exception {
        PgBouncerLab.dropServerConnections();
        try (Connection d = PgBouncerLab.direct(); Statement st = d.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS job_lock (name text PRIMARY KEY, owner text, token bigint NOT NULL, expires_at timestamptz NOT NULL)");
            st.execute("CREATE TABLE IF NOT EXISTS job_result (id bigserial PRIMARY KEY, job text NOT NULL, token bigint NOT NULL)");
            st.execute("TRUNCATE job_lock, job_result");
        }
    }

    @Test
    void 트랜잭션_단위_잠금은_다른_서버_연결에서도_정확히_막는다() throws Exception {
        try (Connection worker = viaBouncer(); Connection other = viaBouncer()) {
            worker.setAutoCommit(false);
            queryBoolean(worker, "SELECT true FROM (SELECT pg_advisory_xact_lock(7)) t");

            assertThat(queryBoolean(other, "SELECT pg_try_advisory_xact_lock(7)")).isFalse();

            worker.commit();
            assertThat(queryBoolean(other, "SELECT pg_try_advisory_xact_lock(7)")).isTrue();
        }
    }

    @Test
    void 작업_전체를_트랜잭션에_넣으면_그동안_서버_연결을_점유한다() throws Exception {
        try (Connection client = viaBouncer()) {
            long baseline = timeSelectOne(client);

            // 작업 2개가 각자 트랜잭션 안에서 잠금을 잡고 2초간 일한다. 서버 연결 2개가 모두 묶인다.
            long whileXactJobs = timeSelectOneWhile(client, 2, key -> {
                Connection c = viaBouncer();
                c.setAutoCommit(false);
                queryBoolean(c, "SELECT true FROM (SELECT pg_advisory_xact_lock(" + key + ")) t");
                return c;
            }, c -> {
                queryBoolean(c, "SELECT true FROM (SELECT pg_sleep(" + JOB.toSeconds() + ")) t");
                c.commit();
            });

            // 같은 작업을 리스로: 획득은 짧은 트랜잭션, 일은 DB 밖, 결과 쓰기만 다시 짧은 트랜잭션.
            long whileLeaseJobs = timeSelectOneWhile(client, 2, key -> {
                Connection c = viaBouncer();
                LeaseLock.tryAcquire(c, "job-" + key, "pod-" + key, Duration.ofSeconds(10)).orElseThrow();
                return c;
            }, c -> {
                Thread.sleep(JOB.toMillis());
                // 결과 쓰기는 생략: 이 실험은 "일하는 동안" 연결 점유만 본다
            });

            log("다른 요청의 SELECT 1 지연: 평소 %dms, 트랜잭션 잠금 작업 중 %dms, 리스 작업 중 %dms",
                    baseline, whileXactJobs, whileLeaseJobs);

            assertThat(whileXactJobs).isGreaterThan(JOB.toMillis() / 2);
            assertThat(whileLeaseJobs).isLessThan(JOB.toMillis() / 4);
        }
    }

    interface Start {
        Connection begin(int key) throws Exception;
    }

    interface Work {
        void run(Connection c) throws Exception;
    }

    /** 작업 n개가 잠금을 잡은 직후, 다른 클라이언트의 SELECT 1이 얼마나 기다리는지 잰다. */
    private long timeSelectOneWhile(Connection client, int n, Start start, Work work) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch locked = new CountDownLatch(n);
        List<Future<?>> jobs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int key = 100 + i;
            jobs.add(pool.submit(() -> {
                Connection c = start.begin(key);
                locked.countDown();
                try (c) {
                    work.run(c);
                }
                return null;
            }));
        }
        assertThat(locked.await(10, TimeUnit.SECONDS)).isTrue();
        long elapsed = timeSelectOne(client);
        for (Future<?> job : jobs) {
            job.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();
        return elapsed;
    }

    /** 이미 연결된 클라이언트(애플리케이션 커넥션 풀의 연결과 같음)에서 SELECT 한 번이 걸리는 시간. */
    private long timeSelectOne(Connection client) throws Exception {
        long start = System.nanoTime();
        queryBoolean(client, "SELECT true");
        return Duration.ofNanos(System.nanoTime() - start).toMillis();
    }
}
