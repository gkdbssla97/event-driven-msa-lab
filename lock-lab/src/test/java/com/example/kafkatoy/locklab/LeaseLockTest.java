package com.example.kafkatoy.locklab;

import static com.example.kafkatoy.locklab.PgBouncerLab.log;
import static com.example.kafkatoy.locklab.PgBouncerLab.queryLong;
import static com.example.kafkatoy.locklab.PgBouncerLab.viaBouncer;
import static org.assertj.core.api.Assertions.assertThat;

import com.example.kafkatoy.locklab.LeaseLock.Lease;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 실험 3. 테이블 리스 + 펜싱 토큰. 모든 연결은 PgBouncer(transaction 모드)를 거친다.
 */
class LeaseLockTest {

    private static final String JOB = "sync";

    @BeforeEach
    void setUp() throws Exception {
        PgBouncerLab.dropServerConnections();
        try (Connection d = PgBouncerLab.direct(); Statement st = d.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS job_lock (name text PRIMARY KEY, owner text, token bigint NOT NULL, expires_at timestamptz NOT NULL)");
            st.execute("CREATE TABLE IF NOT EXISTS job_result (id bigserial PRIMARY KEY, job text NOT NULL, token bigint NOT NULL)");
            st.execute("TRUNCATE job_lock, job_result");
        }
    }

    @Test
    void 동시에_10개가_획득해도_하나만_성공한다() throws Exception {
        int pods = 10;
        ExecutorService pool = Executors.newFixedThreadPool(pods);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<Optional<Lease>>> results = new ArrayList<>();
        for (int i = 0; i < pods; i++) {
            String owner = "pod-" + i;
            results.add(pool.submit(() -> {
                try (Connection c = viaBouncer()) {
                    go.await();
                    return LeaseLock.tryAcquire(c, JOB, owner, Duration.ofSeconds(5));
                }
            }));
        }
        go.countDown();
        long winners = 0;
        for (Future<Optional<Lease>> r : results) {
            if (r.get(10, TimeUnit.SECONDS).isPresent()) {
                winners++;
            }
        }
        pool.shutdown();
        log("동시 획득 시도 %d개 중 성공 %d개", pods, winners);
        assertThat(winners).isEqualTo(1);
    }

    @Test
    void 만료되면_다음_파드가_더_큰_토큰으로_가져가고_옛_소유자는_막힌다() throws Exception {
        try (Connection podA = viaBouncer(); Connection podB = viaBouncer()) {
            Lease a = LeaseLock.tryAcquire(podA, JOB, "pod-a", Duration.ofSeconds(1)).orElseThrow();
            assertThat(LeaseLock.tryAcquire(podB, JOB, "pod-b", Duration.ofSeconds(1))).isEmpty();

            Thread.sleep(1300); // 파드 A가 멈춘 사이 리스 만료

            Lease b = LeaseLock.tryAcquire(podB, JOB, "pod-b", Duration.ofSeconds(5)).orElseThrow();
            log("파드 A 토큰=%d, 만료 후 파드 B 토큰=%d", a.token(), b.token());
            assertThat(b.token()).isEqualTo(a.token() + 1);

            // 깨어난 파드 A: 갱신도, 결과 쓰기도 거부된다.
            assertThat(LeaseLock.renew(podA, a, Duration.ofSeconds(5))).isFalse();
            assertThat(LeaseLock.fencedWrite(podA, a, c -> insertResult(c, a))).isFalse();
            assertThat(LeaseLock.fencedWrite(podB, b, c -> insertResult(c, b))).isTrue();

            long staleRows = queryLong(podB, "SELECT count(*) FROM job_result WHERE token = " + a.token());
            long rows = queryLong(podB, "SELECT count(*) FROM job_result");
            log("결과 행: 전체 %d개, 옛 토큰으로 쓴 행 %d개", rows, staleRows);
            assertThat(staleRows).isZero();
            assertThat(rows).isEqualTo(1);
        }
    }

    @Test
    void 결과를_쓰는_동안에는_만료돼도_다른_파드가_가져가지_못한다() throws Exception {
        try (Connection podA = viaBouncer(); Connection podB = viaBouncer()) {
            Lease a = LeaseLock.tryAcquire(podA, JOB, "pod-a", Duration.ofSeconds(1)).orElseThrow();

            ExecutorService bg = Executors.newSingleThreadExecutor();
            CountDownLatch checked = new CountDownLatch(1);
            // 파드 A: 토큰 검사(FOR SHARE) 직후 1.5초 동안 쓰기를 끌어 리스 만료 시각을 넘긴다.
            Future<Boolean> write = bg.submit(() -> LeaseLock.fencedWrite(podA, a, c -> {
                checked.countDown();
                sleep(1500);
                insertResult(c, a);
            }));
            assertThat(checked.await(5, TimeUnit.SECONDS)).isTrue();
            sleep(1100); // DB 기준으로 리스가 만료된 시점

            long start = System.nanoTime();
            Optional<Lease> b = LeaseLock.tryAcquire(podB, JOB, "pod-b", Duration.ofSeconds(5));
            long waitedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

            assertThat(write.get(5, TimeUnit.SECONDS)).isTrue();
            bg.shutdown();
            log("파드 B의 획득은 파드 A의 쓰기 커밋까지 %dms 기다린 뒤 성공=%s", waitedMs, b.isPresent());

            // 파드 B의 획득 UPDATE는 FOR SHARE 행 잠금을 기다렸다가, 커밋 후 만료를 확인하고 가져간다.
            assertThat(waitedMs).isGreaterThan(200);
            assertThat(b).isPresent();
            assertThat(b.get().token()).isEqualTo(a.token() + 1);
        }
    }

    @Test
    void 소유자가_아닌_쪽의_해제는_무시된다() throws Exception {
        try (Connection podA = viaBouncer(); Connection podB = viaBouncer()) {
            Lease a = LeaseLock.tryAcquire(podA, JOB, "pod-a", Duration.ofSeconds(5)).orElseThrow();

            assertThat(LeaseLock.release(podB, new Lease(JOB, "pod-b", a.token()))).isFalse();
            assertThat(LeaseLock.tryAcquire(podB, JOB, "pod-b", Duration.ofSeconds(5))).isEmpty();

            assertThat(LeaseLock.release(podA, a)).isTrue();
            Lease b = LeaseLock.tryAcquire(podB, JOB, "pod-b", Duration.ofSeconds(5)).orElseThrow();
            assertThat(b.token()).isEqualTo(a.token() + 1);
        }
    }

    @Test
    void 갱신하는_동안에는_TTL이_지나도_리스를_지킨다() throws Exception {
        Duration ttl = Duration.ofSeconds(1);
        try (Connection podA = viaBouncer(); Connection keeperConn = viaBouncer(); Connection podB = viaBouncer()) {
            Lease a = LeaseLock.tryAcquire(podA, JOB, "pod-a", ttl).orElseThrow();
            try (LeaseKeeper keeper = new LeaseKeeper(keeperConn, a, ttl, Duration.ofMillis(300))) {
                for (int i = 0; i < 15; i++) { // 3초, TTL의 3배
                    sleep(200);
                    assertThat(keeper.isLost()).isFalse();
                    assertThat(LeaseLock.tryAcquire(podB, JOB, "pod-b", ttl)).isEmpty();
                }
            }
            log("TTL %dms, 갱신 주기 300ms로 3000ms 동안 리스 유지", ttl.toMillis());
        }
    }

    @Test
    void 갱신이_계속_실패하면_DB_만료보다_먼저_스스로_멈춘다() throws Exception {
        Duration ttl = Duration.ofSeconds(2);
        try (Connection podA = viaBouncer(); Connection podB = viaBouncer()) {
            Lease a = LeaseLock.tryAcquire(podA, JOB, "pod-a", ttl).orElseThrow();
            long acquiredAt = System.nanoTime();

            Connection brokenConn = viaBouncer();
            brokenConn.close(); // 갱신용 연결이 끊긴 상황: 갱신은 매번 예외
            try (LeaseKeeper keeper = new LeaseKeeper(brokenConn, a, ttl, Duration.ofMillis(500))) {
                while (!keeper.isLost()) {
                    sleep(50);
                }
                long stoppedAfterMs = Duration.ofNanos(System.nanoTime() - acquiredAt).toMillis();

                // 스스로 멈춘 시점에는 DB 리스가 아직 살아 있어 아무도 가져가지 못했다.
                boolean takenWhenStopped = LeaseLock.tryAcquire(podB, JOB, "pod-b", ttl).isPresent();
                log("갱신 실패 후 스스로 멈춘 시점 %dms (TTL %dms), 그 시점에 다른 파드가 가져갔나=%s",
                        stoppedAfterMs, ttl.toMillis(), takenWhenStopped);
                assertThat(stoppedAfterMs).isLessThan(ttl.toMillis());
                assertThat(takenWhenStopped).isFalse();
            }

            sleep(800);
            assertThat(LeaseLock.tryAcquire(podB, JOB, "pod-b", ttl)).isPresent();
        }
    }

    private static void insertResult(Connection c, Lease lease) throws java.sql.SQLException {
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO job_result (job, token) VALUES (?, ?)")) {
            ps.setString(1, lease.name());
            ps.setLong(2, lease.token());
            ps.executeUpdate();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
