-- ShedLock: 분산 스케줄러 락 테이블
-- MySQL에서는 밀리초 정밀도(TIMESTAMP(3))를 써야 lockAtLeastFor 같은
-- 초 미만 락 계산이 어긋나지 않는다 (ShedLock 공식 권장).
CREATE TABLE IF NOT EXISTS shedlock (
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMP(3) NOT NULL,
    locked_at  TIMESTAMP(3) NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);
