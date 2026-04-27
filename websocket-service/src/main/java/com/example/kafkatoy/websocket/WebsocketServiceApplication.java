package com.example.kafkatoy.websocket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class WebsocketServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebsocketServiceApplication.class, args);
    }
}
