package com.example.kafkatoy.websocket;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import com.example.kafkatoy.contracts.PaymentFailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class PaymentUpdateBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(PaymentUpdateBroadcaster.class);

    private final SimpMessagingTemplate messagingTemplate;

    public PaymentUpdateBroadcaster(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void broadcastCompleted(PaymentCompletedEvent event) {
        Map<String, String> message = Map.of(
                "orderId", event.orderId(),
                "status", "CONFIRMED",
                "eventType", event.eventType()
        );
        String destination = destinationFor(event.orderId());
        messagingTemplate.convertAndSend(destination, message);
        log.info("Broadcast CONFIRMED to {}", destination);
    }

    public void broadcastFailed(PaymentFailedEvent event) {
        Map<String, String> message = Map.of(
                "orderId", event.orderId(),
                "status", "CANCELED",
                "eventType", event.eventType(),
                "reason", event.failureReason() != null ? event.failureReason() : ""
        );
        String destination = destinationFor(event.orderId());
        messagingTemplate.convertAndSend(destination, message);
        log.info("Broadcast CANCELED to {}", destination);
    }

    // orderId 기준 구독 — 같은 주문 상태를 구독한 모든 클라이언트에게 전달
    static String destinationFor(String orderId) {
        return "/topic/orders/" + orderId;
    }
}
