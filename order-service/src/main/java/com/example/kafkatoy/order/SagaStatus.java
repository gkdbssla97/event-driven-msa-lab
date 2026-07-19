package com.example.kafkatoy.order;

public enum SagaStatus {
    STARTED,
    RESERVED,
    COMPLETED,
    FAILED_INSUFFICIENT_STOCK,
    COMPENSATED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED_INSUFFICIENT_STOCK || this == COMPENSATED;
    }
}
