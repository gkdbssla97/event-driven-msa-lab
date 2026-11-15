package com.example.kafkatoy.locklab;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * PostgreSQL 앞에 PgBouncer(transaction 모드, 서버 연결 2개)를 둔 실험 환경.
 *
 * <p>애플리케이션의 "파드"는 PgBouncer에 붙은 클라이언트 연결 하나로 흉내 낸다.
 * transaction 모드에서는 트랜잭션이 끝날 때마다 서버 연결이 풀로 돌아가므로,
 * 같은 클라이언트라도 다음 트랜잭션은 다른 서버 연결(다른 backend pid)에서 실행될 수 있다.
 */
final class PgBouncerLab {

    static final int SERVER_POOL_SIZE = 2;

    private static final Network NETWORK = Network.newNetwork();

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withNetwork(NETWORK)
            .withNetworkAliases("pg")
            .withDatabaseName("lab")
            .withUsername("lab")
            .withPassword("lab");

    static final GenericContainer<?> PGBOUNCER = new GenericContainer<>("edoburu/pgbouncer:v1.25.1-p0")
            .withNetwork(NETWORK)
            .withEnv(Map.of(
                    "DB_HOST", "pg",
                    "DB_NAME", "lab",
                    "DB_USER", "lab",
                    "DB_PASSWORD", "lab",
                    "AUTH_TYPE", "scram-sha-256",
                    "POOL_MODE", "transaction",
                    "DEFAULT_POOL_SIZE", String.valueOf(SERVER_POOL_SIZE),
                    "MAX_CLIENT_CONN", "100"))
            .withExposedPorts(5432)
            .dependsOn(POSTGRES)
            .waitingFor(Wait.forListeningPort());

    static {
        POSTGRES.start();
        PGBOUNCER.start();
    }

    private PgBouncerLab() {
    }

    /**
     * PgBouncer를 거치는 연결("파드").
     * prepareThreshold=0: transaction 모드에서 서버 측 prepared statement가 다른 서버 연결에 없어서 나는 오류를 피한다.
     */
    static Connection viaBouncer() throws SQLException {
        String url = "jdbc:postgresql://%s:%d/lab?prepareThreshold=0"
                .formatted(PGBOUNCER.getHost(), PGBOUNCER.getMappedPort(5432));
        return DriverManager.getConnection(url, "lab", "lab");
    }

    /** PostgreSQL에 직접 붙는 관찰용 연결. pg_locks, pg_stat_activity 조회에 쓴다. */
    static Connection direct() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), "lab", "lab");
    }

    /** 지금 이 쿼리를 실행한 서버 연결의 backend pid. */
    static long backendPid(Connection c) throws SQLException {
        return queryLong(c, "SELECT pg_backend_pid()");
    }

    /** advisory lock 키를 잡고 있는 서버 연결 pid 목록. */
    static List<Long> advisoryLockHolders(Connection direct, long key) throws SQLException {
        String sql = "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objid = " + key + " AND granted";
        List<Long> pids = new ArrayList<>();
        try (Statement st = direct.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                pids.add(rs.getLong(1));
            }
        }
        return pids;
    }

    /** 실험 사이에 서버 연결을 모두 끊어, 앞 실험의 세션 잠금이 남지 않게 한다. */
    static void dropServerConnections() throws Exception {
        try (Connection d = direct(); Statement st = d.createStatement()) {
            st.execute("""
                    SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                     WHERE datname = 'lab' AND backend_type = 'client backend' AND pid <> pg_backend_pid()
                    """);
        }
        Thread.sleep(300);
    }

    static long queryLong(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    static boolean queryBoolean(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getBoolean(1);
        }
    }

    static void log(String format, Object... args) {
        System.out.println("[lab] " + format.formatted(args));
    }
}
