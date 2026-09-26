package com.example.kafkatoy.order;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * "send()는 비동기다"라는 말의 경계를 재는 실험.
 *
 * 두 실험 모두 브로커가 필요 없다. 크기 검사도 메타데이터 대기도
 * 프로듀서 클라이언트 안에서 일어나기 때문이다.
 *
 * 핵심: send()는 실패해도 예외를 "던지지" 않는다. 실패한 Future를 돌려준다.
 *      대신 어떤 실패는 즉시 돌아오고, 어떤 실패는 max.block.ms를 다 쓰고 돌아온다.
 */
@Testcontainers
class KafkaProducerLimitsTest {

    /** 실험1에는 살아 있는 브로커가 필요하다. 메타데이터를 받아야 크기 검사까지 도달하기 때문이다. */
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    /** 응답이 없는 주소. 네트워크까지 가더라도 메타데이터를 받을 수 없다. */
    private static final String DEAD_BROKER = "localhost:1";

    /** 버퍼 포화를 재현하기 위한 프로듀서: 버퍼는 작게, 레코드 한도는 크게. */
    private KafkaProducer<String, String> smallBufferProducer(long maxBlockMs, int bufferBytes) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, (long) bufferBytes);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 5 * 1024 * 1024); // 레코드 한도는 올려둔 상태
        return new KafkaProducer<>(props);
    }

    @Test
    void 버퍼가_차면_뒤따르는_send_가_max_block_ms_만큼_막힌다() throws Exception {
        String oneMb = "x".repeat(1024 * 1024);

        try (KafkaProducer<String, String> producer = smallBufferProducer(3_000, 2 * 1024 * 1024)) {
            producer.partitionsFor("demo-topic"); // 메타데이터 확보

            // 브로커를 멈춰 Sender가 버퍼를 비우지 못하게 만든다
            DockerClientFactory.instance().client()
                    .pauseContainerCmd(KAFKA.getContainerId()).exec();
            try {
                producer.send(new ProducerRecord<>("demo-topic", oneMb));
                producer.send(new ProducerRecord<>("demo-topic", oneMb));

                long t0 = System.currentTimeMillis();
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>("demo-topic", oneMb));
                long elapsed = System.currentTimeMillis() - t0;

                Throwable cause = null;
                try {
                    future.get();
                } catch (ExecutionException e) {
                    cause = e.getCause();
                }
                System.out.printf("[실험3] 버퍼 포화 → send() 반환까지 %dms, 원인=%s%n",
                        elapsed, cause == null ? "없음" : cause.getMessage());

                assertThat(elapsed).isGreaterThanOrEqualTo(2_500);
                assertThat(cause).isInstanceOf(TimeoutException.class);
                assertThat(cause.getMessage()).contains("Failed to allocate");
            } finally {
                DockerClientFactory.instance().client()
                        .unpauseContainerCmd(KAFKA.getContainerId()).exec();
            }
        }
    }

    private KafkaProducer<String, String> producer(long maxBlockMs) {
        return producer(maxBlockMs, DEAD_BROKER);
    }

    private KafkaProducer<String, String> producer(long maxBlockMs, String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        return new KafkaProducer<>(props);
    }

    @Test
    void 크기_초과는_max_block_ms_와_무관하게_즉시_실패한다() {
        String payload = "x".repeat(2 * 1024 * 1024); // 2MB, 기본 max.request.size = 1MiB

        try (KafkaProducer<String, String> producer = producer(60_000, KAFKA.getBootstrapServers())) {
            // 메타데이터를 미리 받아둔다(첫 호출의 메타데이터 대기를 실험에서 배제)
            producer.partitionsFor("demo-topic");

            long t0 = System.currentTimeMillis();
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("demo-topic", payload));
            long elapsed = System.currentTimeMillis() - t0;

            System.out.printf("[실험1] send() 반환까지 %dms, 이미 완료됨=%s%n", elapsed, future.isDone());
            assertThatThrownBy(future::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(RecordTooLargeException.class);

            // max.block.ms가 60초인데도 기다리지 않는다
            assertThat(elapsed).isLessThan(1_000);
            assertThat(future.isDone()).isTrue();
        }
    }

    @Test
    void 메타데이터를_못_받으면_send_가_max_block_ms_만큼_막힌다() {
        try (KafkaProducer<String, String> producer = producer(5_000)) {
            long t0 = System.currentTimeMillis();
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("demo-topic", "hello"));
            long elapsed = System.currentTimeMillis() - t0;

            System.out.printf("[실험2] send() 반환까지 %dms (max.block.ms=5000)%n", elapsed);
            assertThatThrownBy(future::get)
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TimeoutException.class);

            // 비동기 API인데 호출 스레드가 5초를 그대로 잡힌다
            assertThat(elapsed).isGreaterThanOrEqualTo(4_500);
        }
    }
}
