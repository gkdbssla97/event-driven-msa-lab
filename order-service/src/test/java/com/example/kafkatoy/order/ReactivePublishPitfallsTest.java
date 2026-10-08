package com.example.kafkatoy.order;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 리액티브 요청 흐름 안에서 Kafka를 발행할 때 생기는 두 가지 함정.
 * 브로커가 필요 없다(메타데이터를 못 받는 상황을 만들기 위해 죽은 주소를 쓴다).
 */
class ReactivePublishPitfallsTest {

    private static final String DEAD_BROKER = "localhost:1";

    private KafkaProducer<String, String> producer(long maxBlockMs) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEAD_BROKER);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        return new KafkaProducer<>(p);
    }

    /** send()를 CompletableFuture로 감싼다. send()를 실제로 호출한 스레드 이름을 기록한다. */
    private CompletableFuture<RecordMetadata> send(KafkaProducer<String, String> producer,
                                                   AtomicReference<String> sendThread) {
        CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
        sendThread.set(Thread.currentThread().getName());
        producer.send(new ProducerRecord<>("demo", "hello"), (meta, ex) -> {
            if (ex != null) cf.completeExceptionally(ex);
            else cf.complete(meta);
        });
        return cf;
    }

    @Test
    void 인자로_바로_넘기면_파이프라인을_조립하는_스레드가_막힌다() {
        try (var producer = producer(3_000)) {
            AtomicReference<String> sendThread = new AtomicReference<>();
            String caller = Thread.currentThread().getName();

            long t0 = System.currentTimeMillis();
            Mono<RecordMetadata> mono = Mono.fromFuture(send(producer, sendThread)); // 인자가 먼저 평가된다
            long assembly = System.currentTimeMillis() - t0;

            System.out.printf("[실험D] 조립에 %dms, send()를 실행한 스레드=%s (호출 스레드=%s)%n",
                    assembly, sendThread.get(), caller);

            assertThat(assembly).isGreaterThanOrEqualTo(2_500);
            assertThat(sendThread.get()).isEqualTo(caller);
            mono.onErrorResume(e -> Mono.empty()).block(Duration.ofSeconds(5));
        }
    }

    @Test
    void 람다로_미루고_subscribeOn을_주면_조립_스레드는_막히지_않는다() {
        try (var producer = producer(3_000)) {
            AtomicReference<String> sendThread = new AtomicReference<>();
            String caller = Thread.currentThread().getName();
            Mono.just(1).subscribeOn(Schedulers.boundedElastic()).block(); // 스케줄러 예열

            long t0 = System.currentTimeMillis();
            Mono<RecordMetadata> mono = Mono.fromFuture(() -> send(producer, sendThread))
                    .subscribeOn(Schedulers.boundedElastic());
            long assembly = System.currentTimeMillis() - t0;

            mono.onErrorResume(e -> Mono.empty()).block(Duration.ofSeconds(10));

            System.out.printf("[실험E] 조립에 %dms, send()를 실행한 스레드=%s (호출 스레드=%s)%n",
                    assembly, sendThread.get(), caller);

            assertThat(assembly).isLessThan(500);
            assertThat(sendThread.get()).startsWith("boundedElastic");
        }
    }

    private List<String> observe(Mono<?> source) throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        Hooks.onErrorDropped(e -> log.add("onErrorDropped(" + e.getClass().getSimpleName() + ")"));
        try {
            Disposable subscription = source
                    .doOnSuccess(v -> log.add("doOnSuccess"))
                    .doOnError(e -> log.add("doOnError(" + e.getClass().getSimpleName() + ")"))
                    .doOnCancel(() -> log.add("doOnCancel"))
                    .doFinally(signal -> log.add("doFinally:" + signal))
                    .subscribe(v -> { }, e -> { });
            Thread.sleep(1_000);      // 아직 끝나지 않은 상태
            subscription.dispose();   // 앞단(게이트웨이)이 먼저 연결을 끊은 상황
            Thread.sleep(1_000);
            return log;
        } finally {
            Hooks.resetOnErrorDropped();
        }
    }

    @Test
    void 취소_F1_논블로킹_대기_중이면_성공도_실패도_아닌_취소만_온다() throws Exception {
        // 끝나지 않는 Future를 기다리는 중(브로커 응답 대기 상황)
        List<String> log = observe(Mono.fromFuture(new CompletableFuture<RecordMetadata>()));
        System.out.printf("[실험F1] 논블로킹 대기 중 취소 → %s%n", log);
        assertThat(log).containsExactly("doOnCancel", "doFinally:cancel");
    }

    @Test
    void 취소_F2_블로킹_send_중이면_워커가_인터럽트되고_에러가_뒤늦게_올_수_있다() throws Exception {
        try (var producer = producer(30_000)) {
            List<String> log = observe(Mono
                    .fromCallable(() -> producer.send(new ProducerRecord<>("demo", "hello")).get())
                    .subscribeOn(Schedulers.boundedElastic()));
            System.out.printf("[실험F2] 블로킹 send 중 취소 → %s%n", log);
            // 취소가 먼저 관측되고, 인터럽트된 send()의 예외가 뒤늦게 흘러온 뒤 최종적으로는 버려진다
            assertThat(log).startsWith("doOnCancel", "doFinally:cancel");
            assertThat(log).contains("doOnError(InterruptException)", "onErrorDropped(InterruptException)");
            assertThat(log).doesNotContain("doOnSuccess");
        }
    }
}
