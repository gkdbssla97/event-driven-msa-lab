package com.example.kafkatoy.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "payment_records")
public class PaymentRecord {

    @Id
    private String orderId;

    private String userId;
    private String status;
    private Instant processedAt;

    protected PaymentRecord() {}

    public static PaymentRecord success(String orderId, String userId) {
        PaymentRecord record = new PaymentRecord();
        record.orderId = orderId;
        record.userId = userId;
        record.status = "SUCCESS";
        record.processedAt = Instant.now();
        return record;
    }

    public String getOrderId() { return orderId; }
    public String getUserId() { return userId; }
    public String getStatus() { return status; }
}
