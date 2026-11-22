package com.example.kafkatoy.locklab;

import static com.example.kafkatoy.locklab.PgBouncerLab.advisoryLockHolders;
import static com.example.kafkatoy.locklab.PgBouncerLab.backendPid;
import static com.example.kafkatoy.locklab.PgBouncerLab.direct;
import static com.example.kafkatoy.locklab.PgBouncerLab.log;
import static com.example.kafkatoy.locklab.PgBouncerLab.queryBoolean;
import static com.example.kafkatoy.locklab.PgBouncerLab.queryLong;
import static com.example.kafkatoy.locklab.PgBouncerLab.viaBouncer;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * 실험 1. 세션 단위 advisory lock(pg_advisory_lock 계열)은 PgBouncer transaction 모드에서 깨진다.
 *
 * <p>세션 잠금은 "클라이언트(파드)"가 아니라 "서버 연결(backend)"에 붙는다. 트랜잭션이 끝나면 그 서버 연결은
 * 잠금을 단 채로 풀에 돌아가고, 다음에 그 연결을 받는 다른 클라이언트는 같은 세션으로 취급된다.
 */
class AdvisoryLockBehindPgBouncerTest {

    private static final long KEY = 42;

    @BeforeEach
    @AfterEach
    void cleanUp() throws Exception {
        PgBouncerLab.dropServerConnections();
    }

    @Test
    void 잠금이_남은_서버_연결을_받은_다른_파드는_잠금_없이도_통과한다() throws Exception {
        try (Connection podA = viaBouncer(); Connection podB = viaBouncer(); Connection podC = viaBouncer();
             Connection observer = direct()) {

            // 파드 A가 잠금을 잡는다. autocommit이라 이 문장이 끝나면 서버 연결은 풀로 돌아간다.
            long lockPid = queryLong(podA, "SELECT pg_backend_pid() FROM (SELECT pg_advisory_lock(" + KEY + ")) t");
            log("파드 A가 잠금 획득, 서버 연결 pid=%d", lockPid);

            // 파드 A의 트랜잭션은 끝났지만 잠금은 그 서버 연결에 그대로 있다.
            assertThat(advisoryLockHolders(observer, KEY)).containsExactly(lockPid);

            // 파드 B, C가 트랜잭션을 열어 서버 연결 2개를 하나씩 차지한다.
            podB.setAutoCommit(false);
            podC.setAutoCommit(false);
            long pidB = backendPid(podB);
            long pidC = backendPid(podC);
            assertThat(new long[]{pidB, pidC}).contains(lockPid);

            Connection onLockServer = pidB == lockPid ? podB : podC;
            Connection onOtherServer = pidB == lockPid ? podC : podB;

            boolean passed = queryBoolean(onLockServer, "SELECT pg_try_advisory_lock(" + KEY + ")");
            boolean blocked = !queryBoolean(onOtherServer, "SELECT pg_try_advisory_lock(" + KEY + ")");
            log("잠금이 남은 서버 연결(pid=%d)을 받은 파드: 획득=%s", lockPid, passed);
            log("다른 서버 연결을 받은 파드: 획득=%s", !blocked);

            // 잠금을 잡은 적 없는 파드가 "같은 세션"이라 재진입으로 통과한다. 파드 A와 동시에 실행하게 된다.
            assertThat(passed).isTrue();
            assertThat(blocked).isTrue();

            podB.rollback();
            podC.rollback();
        }
    }

    @Test
    void 해제가_다른_서버_연결로_가면_실패하고_잠금은_남는다() throws Exception {
        try (Connection podA = viaBouncer(); Connection podB = viaBouncer(); Connection podC = viaBouncer();
             Connection observer = direct()) {

            long lockPid = queryLong(podA, "SELECT pg_backend_pid() FROM (SELECT pg_advisory_lock(" + KEY + ")) t");
            log("파드 A가 잠금 획득, 서버 연결 pid=%d", lockPid);

            // 잠금이 남은 서버 연결을 다른 파드의 트랜잭션이 쓰고 있게 만든다.
            podB.setAutoCommit(false);
            Connection holder = podB;
            if (backendPid(podB) != lockPid) {
                podC.setAutoCommit(false);
                assertThat(backendPid(podC)).isEqualTo(lockPid);
                podB.rollback();
                holder = podC;
            }

            // 파드 A의 다음 트랜잭션은 남은 서버 연결로 간다. 해제는 실패한다.
            long unlockPid = backendPid(podA);
            boolean unlocked = queryBoolean(podA, "SELECT pg_advisory_unlock(" + KEY + ")");
            log("파드 A의 해제가 실행된 서버 연결 pid=%d, 해제 성공=%s", unlockPid, unlocked);

            assertThat(unlockPid).isNotEqualTo(lockPid);
            assertThat(unlocked).isFalse();
            assertThat(advisoryLockHolders(observer, KEY)).containsExactly(lockPid);
            log("해제 후에도 잠금을 잡고 있는 서버 연결 pid=%s", advisoryLockHolders(observer, KEY));

            holder.rollback();
        }
    }
}
