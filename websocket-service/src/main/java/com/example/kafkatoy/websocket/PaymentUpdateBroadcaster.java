package com.example.kafkatoy.websocket;

import com.example.kafkatoy.contracts.PaymentCompletedEvent;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentUpdateBroadcaster {

    private final SimpMessagingTemplate messagingTemplate;

    public PaymentUpdateBroadcaster(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void broadcast(PaymentCompletedEvent event) {
        messagingTemplate.convertAndSend(destinationFor(event.userId()), event);
    }

    static String destinationFor(String userId) {
        return "/topic/payments/" + userId;
    }
}
