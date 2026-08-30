package com.example.kafkatoy.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * 결제 금액을 append-only로 기록하는 원장. 잔액은 이 테이블의 SUM으로 파생하며,
 * 어떤 엔트리도 UPDATE하지 않는다 — PaymentRecord(멱등성 가드)와는 다른 책임이다.
 *
 * id는 orderId+type 자연키라 같은 종류의 엔트리가 중복 기록되지 않는다.
 */
@Entity
@Table(name = "ledger_entries")
public class LedgerEntry {

    @Id
    private String id;

    private String orderId;
    private String userId;

    @Enumerated(EnumType.STRING)
    private LedgerEntryType type;

    private long amount;
    private Instant createdAt;

    protected LedgerEntry() {}

    public static LedgerEntry paid(String orderId, String userId, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
        LedgerEntry entry = new LedgerEntry();
        entry.id = ledgerId(orderId, LedgerEntryType.PAID);
        entry.orderId = orderId;
        entry.userId = userId;
        entry.type = LedgerEntryType.PAID;
        entry.amount = amount;
        entry.createdAt = Instant.now();
        return entry;
    }

    public static String ledgerId(String orderId, LedgerEntryType type) {
        return orderId + ":" + type;
    }

    public String getId() { return id; }
    public String getOrderId() { return orderId; }
    public String getUserId() { return userId; }
    public LedgerEntryType getType() { return type; }
    public long getAmount() { return amount; }
    public Instant getCreatedAt() { return createdAt; }
}
