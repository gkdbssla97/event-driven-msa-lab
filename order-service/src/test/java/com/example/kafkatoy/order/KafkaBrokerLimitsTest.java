package com.example.kafkatoy.order;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 브로커 쪽 한도를 넘겼을 때 클라이언트에 무엇이 보이는지 확인한다.
 * 브로커: message.max.bytes = 1MB, socket.request.max.bytes = 2MB
 * 프로듀서: max.request.size = 10MB (클라이언트 검사는 통과시켜 브로커까지 보낸다)
 */
@Testcontainers
class KafkaBrokerLimitsTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withEnv("KAFKA_MESSAGE_MAX_BYTES", "1048576")        // 1MB
                    .withEnv("KAFKA_SOCKET_REQUEST_MAX_BYTES", "2097152") // 2MB
                    .withEnv("KAFKA_REPLICA_FETCH_MAX_BYTES", "1048576");

    private KafkaProducer<String, String> producer(long deliveryTimeoutMs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10 * 1024 * 1024);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) deliveryTimeoutMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        return new KafkaProducer<>(props);
    }

    @Test
    void 브로커_메시지_한도_초과() throws Exception {
        String payload = "x".repeat((int) (1.5 * 1024 * 1024)); // 1.5MB: 소켓 상한 이내, 메시지 한도 초과
        try (KafkaProducer<String, String> producer = producer(30_000)) {
            producer.partitionsFor("demo-a");
            long t0 = System.currentTimeMillis();
            Future<RecordMetadata> f = producer.send(new ProducerRecord<>("demo-a", payload));
            try {
                f.get();
                System.out.println("[실험4-A] 성공?!");
            } catch (ExecutionException e) {
                System.out.printf("[실험4-A] %dms → %s: %s%n", System.currentTimeMillis() - t0,
                        e.getCause().getClass().getSimpleName(), e.getCause().getMessage());
            }
        }
    }

    @Test
    void 브로커_소켓_상한_초과() throws Exception {
        String payload = "x".repeat(3 * 1024 * 1024); // 3MB: 소켓 상한(2MB) 초과
        try (KafkaProducer<String, String> producer = producer(8_000)) {
            producer.partitionsFor("demo-b");
            long t0 = System.currentTimeMillis();
            Future<RecordMetadata> f = producer.send(new ProducerRecord<>("demo-b", payload));
            try {
                f.get();
                System.out.println("[실험4-B] 성공?!");
            } catch (ExecutionException e) {
                System.out.printf("[실험4-B] %dms → %s: %s%n", System.currentTimeMillis() - t0,
                        e.getCause().getClass().getSimpleName(), e.getCause().getMessage());
            }
        }
    }
}
