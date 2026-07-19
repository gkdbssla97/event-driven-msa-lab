package com.example.kafkatoy.order;

public enum SagaStatus {
    STARTED,
    RESERVED,
    COMPLETED,
    FAILED_INSUFFICIENT_STOCK,
    COMPENSATED,
    TIMED_OUT; // 스위퍼가 "멈춘 사가"로 판단해 자동 취소·보상한 종착 상태

    public boolean isTerminal() {
        return this == COMPLETED
                || this == FAILED_INSUFFICIENT_STOCK
                || this == COMPENSATED
                || this == TIMED_OUT;
    }
}
