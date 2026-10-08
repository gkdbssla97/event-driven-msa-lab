package com.example.kafkatoy.order;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 발행 결과를 기다리지 않으면(fire-and-forget) 어떤 일이 생기는지 확인한다.
 * 브로커는 토픽 자동 생성을 끈 상태로 띄운다(운영 환경과 같은 조건).
 */
@Testcontainers
class KafkaFireAndForgetTest {

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static final String TOPIC = "orders";

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    private KafkaProducer<String, String> producer(long maxBlockMs) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        return new KafkaProducer<>(p);
    }

    private int countRecords(String topic) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + System.nanoTime());
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c)) {
            consumer.subscribe(List.of(topic));
            int total = 0;
            long until = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < until) {
                total += consumer.poll(Duration.ofMillis(500)).count();
            }
            return total;
        }
    }

    /** 흔한 퍼블리셔 모양: 발행하고 결과는 로그로만 남긴 채 곧바로 "성공"을 반환한다. */
    private String fireAndForget(KafkaProducer<String, String> producer, String topic, String payload,
                                 AtomicReference<Throwable> loggedError) {
        producer.send(new ProducerRecord<>(topic, payload), (meta, ex) -> {
            if (ex != null) loggedError.set(ex);      // 실제 코드라면 log.error(...) 한 줄
        });
        return "200 OK";
    }

    @Test
    void 결과를_기다리지_않으면_크기_초과는_200과_함께_조용히_사라진다() throws Exception {
        String twoMb = "x".repeat(2 * 1024 * 1024);
        AtomicReference<Throwable> loggedError = new AtomicReference<>();

        try (var producer = producer(60_000)) {
            producer.partitionsFor(TOPIC);
            long t0 = System.currentTimeMillis();
            String response = fireAndForget(producer, TOPIC, twoMb, loggedError);
            long elapsed = System.currentTimeMillis() - t0;
            producer.flush();

            int delivered = countRecords(TOPIC);
            System.out.printf("[실험A] 응답=%s (%dms), 콜백 오류=%s, 토픽에 도착한 레코드=%d%n",
                    response, elapsed,
                    loggedError.get() == null ? "없음" : loggedError.get().getClass().getSimpleName(),
                    delivered);

            assertThat(response).isEqualTo("200 OK");
            assertThat(loggedError.get()).isNotNull();
            assertThat(delivered).isZero();
        }
    }

    @Test
    void 결과를_기다리면_실패가_호출자에게_드러난다() {
        String twoMb = "x".repeat(2 * 1024 * 1024);
        try (var producer = producer(60_000)) {
            producer.partitionsFor(TOPIC);
            long t0 = System.currentTimeMillis();
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(TOPIC, twoMb)).get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            System.out.printf("[실험B] 결과를 기다림 → %dms 만에 실패를 호출자가 인지%n",
                    System.currentTimeMillis() - t0);
        }
    }

    @Test
    void 결과를_기다리지_않아도_없는_토픽이면_send_자체가_막힌다() {
        AtomicReference<Throwable> loggedError = new AtomicReference<>();
        try (var producer = producer(5_000)) {
            long t0 = System.currentTimeMillis();
            String response = fireAndForget(producer, "orderz", "hello", loggedError); // 오타 토픽
            long elapsed = System.currentTimeMillis() - t0;
            System.out.printf("[실험C] 없는 토픽 → 응답=%s, 반환까지 %dms, 콜백 오류=%s%n",
                    response, elapsed,
                    loggedError.get() == null ? "없음" : loggedError.get().getMessage());

            assertThat(elapsed).isGreaterThanOrEqualTo(4_500);
        }
    }
}
