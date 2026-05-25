package com.example.kafkatoy.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private String id;

    private String userId;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private Instant createdAt;

    protected Order() {}

    public static Order create(String id, String userId) {
        Order order = new Order();
        order.id = id;
        order.userId = userId;
        order.status = OrderStatus.PENDING;
        order.createdAt = Instant.now();
        return order;
    }

    public void confirm() {
        this.status = OrderStatus.CONFIRMED;
    }

    public void cancel() {
        this.status = OrderStatus.CANCELED;
    }

    public String getId() { return id; }
    public String getUserId() { return userId; }
    public OrderStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}
