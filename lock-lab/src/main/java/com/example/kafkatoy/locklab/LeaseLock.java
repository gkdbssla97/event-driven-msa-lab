package com.example.kafkatoy.locklab;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

/**
 * 테이블 기반 리스(lease) 잠금 + 펜싱 토큰.
 *
 * <p>획득·갱신·해제가 모두 "짧은 트랜잭션 하나"로 끝나므로, PgBouncer transaction 모드처럼
 * 트랜잭션마다 서버 연결이 바뀌는 환경에서도 잠금 상태가 연결에 묶이지 않는다.
 * 잠금 상태는 job_lock 행에만 있다.
 *
 * <p>시간 비교는 전부 DB의 now()로 한다. 애플리케이션 시계는 판단에 쓰지 않는다.
 *
 * <pre>
 * CREATE TABLE job_lock (
 *   name       text PRIMARY KEY,
 *   owner      text,
 *   token      bigint NOT NULL,      -- 펜싱 토큰. 획득할 때마다 +1, 되돌리지 않는다
 *   expires_at timestamptz NOT NULL
 * );
 * </pre>
 */
public final class LeaseLock {

    public record Lease(String name, String owner, long token) {
    }

    @FunctionalInterface
    public interface SqlWork {
        void run(Connection connection) throws SQLException;
    }

    private LeaseLock() {
    }

    /**
     * 비어 있거나 만료된 리스만 가져온다. 조회와 갱신을 나누지 않고 SQL 한 문장으로 처리한다.
     * 행이 돌아오면 획득이고, 돌아온 token이 이번 소유의 펜싱 토큰이다.
     */
    public static Optional<Lease> tryAcquire(Connection c, String name, String owner, Duration ttl)
            throws SQLException {
        String sql = """
                INSERT INTO job_lock (name, owner, token, expires_at)
                VALUES (?, ?, 1, now() + make_interval(secs => ?))
                ON CONFLICT (name) DO UPDATE
                   SET owner = EXCLUDED.owner,
                       token = job_lock.token + 1,
                       expires_at = EXCLUDED.expires_at
                 WHERE job_lock.owner IS NULL OR job_lock.expires_at < now()
                RETURNING token
                """;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, name);
            ps.setString(2, owner);
            ps.setDouble(3, seconds(ttl));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(new Lease(name, owner, rs.getLong(1))) : Optional.empty();
            }
        }
    }

    /** 내 owner와 token이 그대로일 때만 연장한다. 영향 행이 0이면 리스를 잃은 것이다. */
    public static boolean renew(Connection c, Lease lease, Duration ttl) throws SQLException {
        String sql = """
                UPDATE job_lock SET expires_at = now() + make_interval(secs => ?)
                 WHERE name = ? AND owner = ? AND token = ?
                """;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setDouble(1, seconds(ttl));
            ps.setString(2, lease.name());
            ps.setString(3, lease.owner());
            ps.setLong(4, lease.token());
            return ps.executeUpdate() == 1;
        }
    }

    /** 내 리스일 때만 비운다. token은 그대로 둔다(줄어들면 펜싱이 깨진다). */
    public static boolean release(Connection c, Lease lease) throws SQLException {
        String sql = """
                UPDATE job_lock SET owner = NULL, expires_at = now()
                 WHERE name = ? AND owner = ? AND token = ?
                """;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, lease.name());
            ps.setString(2, lease.owner());
            ps.setLong(3, lease.token());
            return ps.executeUpdate() == 1;
        }
    }

    /**
     * 펜싱 검사와 결과 쓰기를 같은 트랜잭션에서 한다.
     *
     * <p>잠금 행을 FOR SHARE로 잡아 두므로, 커밋할 때까지 다른 인스턴스가 이 리스를 가져갈 수 없다
     * (획득 UPDATE가 행 잠금을 기다린다). 갱신 쿼리에서만 토큰을 검사하면 "갱신 성공 → 멈춤 → 만료 →
     * 다른 인스턴스 획득 → 깨어나서 쓰기" 틈이 남는다. 이 메서드는 그 틈을 막는다.
     *
     * @return 쓰기에 성공하면 true, 리스를 이미 잃었으면 아무것도 쓰지 않고 false
     */
    public static boolean fencedWrite(Connection c, Lease lease, SqlWork work) throws SQLException {
        boolean previousAutoCommit = c.getAutoCommit();
        c.setAutoCommit(false);
        try {
            String check = """
                    SELECT 1 FROM job_lock
                     WHERE name = ? AND owner = ? AND token = ? AND expires_at > now()
                     FOR SHARE
                    """;
            try (PreparedStatement ps = c.prepareStatement(check)) {
                ps.setString(1, lease.name());
                ps.setString(2, lease.owner());
                ps.setLong(3, lease.token());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        c.rollback();
                        return false;
                    }
                }
            }
            work.run(c);
            c.commit();
            return true;
        } catch (SQLException | RuntimeException e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(previousAutoCommit);
        }
    }

    private static double seconds(Duration d) {
        return d.toMillis() / 1000.0;
    }
}
