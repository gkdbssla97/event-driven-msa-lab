package com.example.kafkatoy.locklab;

import static com.example.kafkatoy.locklab.PgBouncerLab.log;
import static com.example.kafkatoy.locklab.PgBouncerLab.queryLong;
import static com.example.kafkatoy.locklab.PgBouncerLab.viaBouncer;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 실험 4. 리스를 잃어도 멈추지 않는 전략(전략 B): 겹침을 잠금이 아니라 "결과를 쓰는 쪽"에서 흡수한다.
 *
 * <p>상황: 실행 A의 리스가 만료돼 실행 B가 가져갔지만, A는 중간에 끊으면 더 위험한 작업이라 끝까지 간다.
 * 두 실행이 같은 버전을 등록하려 할 때, 결과 쓰기가 멱등한지에 따라 결과가 갈린다.
 */
class IdempotentResultTest {

    private static final String JOB = "sync";
    private static final int VERSION = 7;

    @BeforeEach
    void setUp() throws Exception {
        PgBouncerLab.dropServerConnections();
        try (Connection d = PgBouncerLab.direct(); Statement st = d.createStatement()) {
            // 확인 후 쓰기용: 유니크 제약 없음
            st.execute("CREATE TABLE IF NOT EXISTS registry_checked (id bigserial PRIMARY KEY, job text NOT NULL, version int NOT NULL, object_key text NOT NULL)");
            // 멱등 쓰기용: (job, version)이 곧 식별자
            st.execute("CREATE TABLE IF NOT EXISTS registry_idempotent (job text NOT NULL, version int NOT NULL, object_key text NOT NULL, PRIMARY KEY (job, version))");
            st.execute("TRUNCATE registry_checked, registry_idempotent");
        }
    }

    @Test
    void 확인_후_쓰기는_두_실행이_같은_틈에_확인을_통과하면_둘_다_등록한다() throws Exception {
        try (Connection runA = viaBouncer(); Connection runB = viaBouncer()) {
            // 두 실행이 "등록된 버전보다 높은가"를 거의 동시에 확인한다. 둘 다 아직 아무것도 없다고 본다.
            boolean aPasses = latestVersion(runA) < VERSION;
            boolean bPasses = latestVersion(runB) < VERSION;
            assertThat(aPasses).isTrue();
            assertThat(bPasses).isTrue();

            // 둘 다 자기가 올린 객체로 등록한다.
            insertChecked(runA, "object-from-A");
            insertChecked(runB, "object-from-B");

            long rows = queryLong(runA, "SELECT count(*) FROM registry_checked WHERE job = '" + JOB + "' AND version = " + VERSION);
            log("확인 후 쓰기: 같은 버전 %d 등록 행 %d개 (A, B 둘 다 확인 통과)", VERSION, rows);
            assertThat(rows).isEqualTo(2);
        }
    }

    @Test
    void 유니크_제약으로_쓰기_자체를_멱등하게_하면_한쪽만_남고_진_쪽은_그_사실을_안다() throws Exception {
        try (Connection runA = viaBouncer(); Connection runB = viaBouncer()) {
            // 확인 단계는 위와 똑같이 둘 다 통과한다고 두고, 쓰기만 바꾼다.
            Optional<String> aWon = insertIdempotent(runA, "object-from-A");
            Optional<String> bWon = insertIdempotent(runB, "object-from-B");

            long rows = queryLong(runA, "SELECT count(*) FROM registry_idempotent WHERE job = '" + JOB + "' AND version = " + VERSION);
            log("멱등 쓰기: 같은 버전 %d 등록 행 %d개, A 등록=%s, B 등록=%s",
                    VERSION, rows, aWon.isPresent(), bWon.isPresent());
            log("B는 등록에 실패했으므로 자신이 올린 object-from-B를 정리 대상으로 판단할 수 있다");

            assertThat(rows).isEqualTo(1);
            assertThat(aWon).contains("object-from-A");
            assertThat(bWon).isEmpty();
        }
    }

    private static int latestVersion(Connection c) throws SQLException {
        return (int) queryLong(c, "SELECT coalesce(max(version), 0) FROM registry_checked WHERE job = '" + JOB + "'");
    }

    private static void insertChecked(Connection c, String objectKey) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO registry_checked (job, version, object_key) VALUES (?, ?, ?)")) {
            ps.setString(1, JOB);
            ps.setInt(2, VERSION);
            ps.setString(3, objectKey);
            ps.executeUpdate();
        }
    }

    /** 등록에 성공하면 등록된 객체 키를, 이미 같은 버전이 있으면 empty를 돌려준다. */
    private static Optional<String> insertIdempotent(Connection c, String objectKey) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO registry_idempotent (job, version, object_key) VALUES (?, ?, ?)
                ON CONFLICT (job, version) DO NOTHING
                RETURNING object_key
                """)) {
            ps.setString(1, JOB);
            ps.setInt(2, VERSION);
            ps.setString(3, objectKey);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
            }
        }
    }
}
