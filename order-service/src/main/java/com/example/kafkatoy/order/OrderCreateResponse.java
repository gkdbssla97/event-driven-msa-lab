package com.example.kafkatoy.order;

public record OrderCreateResponse(String orderId, String userId, String status) {
}
