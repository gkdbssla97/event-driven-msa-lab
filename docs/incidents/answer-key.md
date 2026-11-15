# 장애 시나리오 정답표

"장애 원인 추적 도우미"의 평가 기준입니다. 정답이 있는 장애를 주입하고, 진단 리포트(`claude -p` + 읽기 전용 MCP 도구)가 원인을 맞혔는지 이 표로 채점합니다.

- 재현 스크립트: `scripts/incidents/`
- 실행 기록: 스크립트가 `build/incidents/runs/<시나리오>-<UTC시각>.json`에 `scenario`, `injectedAt`, `orderIds`를 남깁니다. 채점할 때 이 파일로 "이번 실행의 정답"을 찾습니다.
- 서비스 로그: `build/incidents/logs/<service>.log`

## 1. 실행 환경

메모리 8GB 맥에서도 돌도록 최소 구성으로 띄웁니다.

| 구성 | 실행 방식 |
|---|---|
| Kafka | 도커 `apache/kafka` KRaft 단일 노드(`edml-kafka`), `auto.create.topics.enable=false` |
| MySQL | `docker-compose.yml`의 `mysql-order`(3306), `mysql-payment`(3307) |
| Redis | 도커 `redis:7-alpine`(`edml-redis`) |
| order / inventory / payment-service | 호스트에서 `java -Xmx256m -jar` 프로세스 |
| websocket-service, Prometheus 서버 | 띄우지 않음. 지표는 각 서비스 `/actuator/prometheus`에서 직접 읽음 |

```bash
./gradlew :order-service:bootJar :payment-service:bootJar :inventory-service:bootJar
scripts/incidents/local-stack.sh up
scripts/incidents/s4-insufficient-stock.sh
scripts/incidents/local-stack.sh down
```

local-stack이 기본값과 다르게 주는 설정입니다.

| 설정 | 기본값 | local-stack | 이유 |
|---|---|---|---|
| `app.saga.timeout-ms` (`SAGA_TIMEOUT_MS`) | 300000 | 60000 | S1·S3를 몇 분 안에 재현 |
| `app.saga.sweep-interval-ms` (`SAGA_SWEEP_INTERVAL_MS`) | 60000 | 10000 | 같은 이유 |
| 브로커 `auto.create.topics.enable` | true (Kafka 기본값. `infra/kafka/values-dev.yaml`에서 지정하지 않음, 차트 자체 기본값은 미확인) | false | S3 증상의 전제 조건 |
| `payment-failed.DLQ` 토픽 | 선언한 곳 없음 | 기동 시 미리 생성 | auto-create=false에서 생기는 무한 재시도를 피하려고(8절 참고) |

### 지표 이름 대응

Micrometer 이름은 Prometheus 노출 시 `.`이 `_`로 바뀌고 Counter에는 `_total`이 붙습니다. 모든 지표에 `application` 태그가 붙어 같은 이름(`outbox_pending` 등)도 서비스별로 구분됩니다.

| Micrometer | Prometheus | 종류 | 등록 위치 |
|---|---|---|---|
| `saga.count{status}` | `saga_count{status="..."}` | Gauge(saga_state 테이블 COUNT, 누적 상태 수) | order `SagaMetrics` |
| `saga.swept` | `saga_swept_total` | Counter | order `SagaTimeoutSweeper` |
| `saga.dlq.recovered` | `saga_dlq_recovered_total` | Counter | order `DlqRecoveryListener` |
| `outbox.pending` | `outbox_pending` | Gauge | order·payment `OutboxDispatcher` |
| `outbox.published` | `outbox_published_total` | Counter | order·payment `OutboxDispatcher` |
| `outbox.publish.failed` | `outbox_publish_failed_total` | Counter | order·payment `OutboxDispatcher` |
| `inventory.reserve{result,backend}` | `inventory_reserve_total{result="success"\|"insufficient_stock"}` | Counter | inventory `RedisInventoryStore` |
| `inventory.compensate{result,backend}` | `inventory_compensate_total{result="restored"\|"not_found"}` | Counter | inventory `RedisInventoryStore` |
| `payment.processed{result}` | `payment_processed_total{result="new"\|"duplicate"}` | Counter | payment `PaymentService` |
| `payment.ledger{type}` | `payment_ledger_total{type="paid"}` | Counter | payment `PaymentService` |

Counter는 프로세스 재시작 시 0부터 다시 셉니다. 스크립트는 주입 직전 값과 비교한 증가분으로 판정합니다.

### 시간대 주의

DB의 `created_at`/`updated_at`은 UTC(`serverTimezone=UTC`)이고, 서비스 로그는 JVM 기본 시간대(이 환경은 `+09:00`)로 찍힙니다. 2단계 도구가 둘을 이어 붙일 때 시간대를 맞춰야 합니다.

## 2. 요약

| ID | 정답 원인 | 사가 종착 상태 | 장애 여부 | 한 줄 판별 근거 |
|---|---|---|---|---|
| S1 | payment-service 중단(프로세스 다운) | `TIMED_OUT` | 장애 | RESERVED에서 멈춤 + payment 쪽에 해당 주문 흔적이 전혀 없음 + payment health 무응답 |
| S2 | order-created 이벤트 payload 손상(역직렬화 불가 poison 메시지) | `FAILED_POISON` | 장애 | `order-created.DLQ` 레코드의 예외 원인이 역직렬화 실패 + `saga_dlq_recovered_total` 증가 |
| S3 | payment-service의 `app.kafka.topics.payment-completed` 오설정(존재하지 않는 토픽) | `TIMED_OUT` | 장애 | 결제는 커밋됐는데 payment outbox에 오타 토픽 PENDING 적체 + `not present in metadata` |
| S4 | 재고 부족(정상적인 비즈니스 실패) | `FAILED_INSUFFICIENT_STOCK` | 장애 아님 | `inventory_reserve_total{result="insufficient_stock"}` 증가, DLQ·스위퍼·보상 관여 없음 |

## 3. S1 payment-service 중지

스크립트: `scripts/incidents/s1-payment-down.sh` (`--no-restore`면 payment-service를 내린 채로 둠)

### 주입 방법
1. payment-service 프로세스를 종료합니다(`local-stack.sh stop payment-service`, k3s라면 `kubectl scale deploy/payment-service --replicas=0`에 해당).
2. 재고가 충분한 상품으로 정상 주문 1건을 넣습니다(`POST /orders`, quantity 1).

### 기대 증상

| 출처 | 기대 값 |
|---|---|
| saga_state | `STARTED` → `RESERVED`(약 2초) → `TIMED_OUT`(RESERVED 이후 `timeout-ms` + 최대 `sweep-interval-ms`) |
| orders | `CANCELED` |
| order outbox_events | `ORDER_CREATED`(order-created, PUBLISHED), `PAYMENT_FAILED`(payment-failed, PUBLISHED). 두 번째 행의 `created_at`이 사가 `updated_at`과 같은 시각 |
| payment DB | 해당 주문의 `payment_records`, `ledger_entries`, `outbox_events` 모두 없음(장애 지속 중) |
| DLQ | 해당 주문 메시지 없음 |
| 지표 | `saga_swept_total` +1, `saga_count{status="TIMED_OUT"}` +1, inventory `inventory_compensate_total{result="restored"}` +1, `saga_dlq_recovered_total` 변화 없음 |
| health | payment-service `/actuator/health` 연결 거부 |
| 로그(order) | `SagaTimeoutSweeper : Saga sweeper: found 1 stuck saga(s) older than 60000ms` / `Saga timed out and compensated: sagaId=<orderId>, lastStatus=RESERVED` |
| 로그(inventory) | `Compensation complete (atomic): orderId=<orderId>, productId=..., quantity=1` |
| 로그(payment) | 장애 구간에 `Received inventory-reserved: orderId=<orderId>` 없음(로그 파일 자체가 멈춤) |

### 정답 원인
payment-service가 내려가 inventory-reserved를 소비하지 못해, 사가가 결제 단계(RESERVED)에서 멈췄다. 스위퍼의 타임아웃 보상은 결과이지 원인이 아니다.

### 원인 판정에 필요한 증거
- 필수: 사가 종착이 `TIMED_OUT`이고 직전 상태가 `RESERVED`(로그 `lastStatus=RESERVED`). 재고 단계는 통과했다는 뜻.
- 필수: payment 쪽에 해당 주문의 흔적이 없음(`payment_records` 행 없음, payment outbox 행 없음).
- 필수: payment-service가 응답하지 않음(health 실패 또는 장애 구간 로그 공백).
- 보조: `saga_swept_total` 증가, DLQ에는 해당 주문 없음.

### 혼동하기 쉬운 오답
- "스위퍼 타임아웃이 너무 짧다"(결과를 원인으로 봄).
- "토픽 오설정"(S3와 사가 상태가 같음. payment DB에 결제 흔적이 없다는 점으로 구분).

### 복구 후 부수 증상
payment-service를 다시 띄우면 밀린 inventory-reserved를 소비해 결제를 처리합니다. `payment_records` SUCCESS와 `ledger_entries` PAID가 생기고 payment-completed도 발행되지만, `OrderService.confirm()`의 종착 가드가 무시해 사가는 `TIMED_OUT`, 재고는 복원된 상태로 남습니다. 즉 "원장상 결제됨 + 주문 취소" 불일치가 생깁니다. 진단 시점이 복구 이후라면 `payment_records.processed_at`이 사가 `updated_at`보다 늦다는 점으로 "늦게 도착한 결제"임을 판별해야 합니다.

## 4. S2 독성(poison) 메시지

스크립트: `scripts/incidents/s2-poison-message.sh`

### 주입 방법
order-service DB에 한 트랜잭션으로 다음 세 행을 직접 넣습니다. "프로듀서 쪽 직렬화 버그로 깨진 이벤트가 Outbox에 적재된" 상황이고, 발행은 실제 `SkipLockedOutboxPublisher`가 합니다.
- `orders`: PENDING
- `saga_state`: STARTED
- `outbox_events`: topic `order-created`, event_type `ORDER_CREATED`, PENDING, payload의 `quantity`가 문자열 `"two"`(`OrderCreatedEvent.quantity`는 `int`). `orderId` 필드는 정상이라 `DlqRecoveryListener`가 뽑을 수 있습니다.

### 기대 증상

| 출처 | 기대 값 |
|---|---|
| saga_state | `STARTED` → `FAILED_POISON`(주입 후 약 5초). `RESERVED`를 거치지 않음 |
| orders | `CANCELED` |
| order outbox_events | `ORDER_CREATED`(PUBLISHED, 발행 자체는 성공), `PAYMENT_FAILED`(payment-failed, PUBLISHED) |
| order-created.DLQ | key=orderId 레코드 1건. 헤더 `kafka_dlt-exception-cause-fqcn:java.lang.IllegalStateException`, `kafka_dlt-exception-message:...OrderCreatedEventListener.handle(java.lang.String)' threw exception; Failed to deserialize order-created event`, `kafka_dlt-original-topic:order-created`, `kafka_dlt-original-consumer-group:inventory-service` |
| payment-failed.DLQ(부수) | key=orderId 레코드 1건. 헤더 `kafka_dlt-exception-cause-fqcn:java.lang.IndexOutOfBoundsException` (8절 결함 1) |
| 지표 | `saga_dlq_recovered_total` +1, `saga_count{status="FAILED_POISON"}` +1, `saga_swept_total` 변화 없음, inventory `inventory_reserve_total` 변화 없음 |
| 로그(inventory) | `KafkaConsumerConfig : Publishing to DLQ: topic=order-created, key=<orderId>, error=Listener method ... threw exception` (재시도 3회 후, 최초 수신부터 약 4초) |
| 로그(order) | `DlqRecoveryListener : Saga compensated from DLQ: orderId=<orderId>, dlqTopic=order-created.DLQ` |
| 로그(inventory, 부수) | `Received payment-failed (compensation trigger): orderId=<orderId>` 4회 후 `Publishing to DLQ: topic=payment-failed, key=<orderId>` |

### 정답 원인
order-created 이벤트의 payload가 스키마와 맞지 않아(quantity 타입 불일치) inventory-service가 역직렬화하지 못했다. 재시도로 해결되지 않는 poison 메시지라 DLQ로 격리됐고, DLQ 복구가 사가를 종결했다.

### 원인 판정에 필요한 증거
- 필수: `order-created.DLQ`에 해당 orderId 레코드가 있고, 예외 메시지가 역직렬화 실패(`Failed to deserialize order-created event`).
- 필수: 사가 종착 `FAILED_POISON`, `saga_dlq_recovered_total` 증가.
- 보조: Outbox의 원본 payload에서 `quantity`가 문자열인 것을 직접 확인.
- 보조: 사가가 `RESERVED`를 거치지 않음(재고 단계 진입 전 실패).

### 혼동하기 쉬운 오답
- "재고 부족"(주문이 취소됐다는 결과만 봄. S4와 달리 DLQ·`FAILED_POISON`이 있음).
- "inventory-service 보상 로직 결함이 원인"(payment-failed.DLQ의 IndexOutOfBoundsException은 2차 증상. 원인 후보에 부수 결함으로 함께 적는 것은 가산점).

## 5. S3 토픽 이름 오설정

스크립트: `scripts/incidents/s3-topic-misconfig.sh` (`S3_BAD_TOPIC`, `S3_ORDERS`로 조정, `--no-restore`면 오설정 상태로 둠)

### 주입 방법
1. 사전 조건: 브로커 `auto.create.topics.enable=false`, 오타 토픽(`payment-complete`)이 없어야 합니다. 스크립트가 확인합니다.
2. payment-service를 `APP_KAFKA_TOPICS_PAYMENT_COMPLETED=payment-complete`로 재기동합니다(`app.kafka.topics.payment-completed` 덮어쓰기).
3. 정상 주문 2건을 넣습니다.

왜 payment-service인지: order-service(`KafkaTopicConfig`)와 inventory-service(`KafkaTopicConfig`)는 발행 토픽을 `NewTopic` 빈으로 선언하므로 이름을 틀려도 KafkaAdmin이 그 이름의 토픽을 만들어 버립니다(발행 성공, 구독자 없음). payment-service에는 `NewTopic` 선언이 없어 토픽이 없으면 프로듀서가 메타데이터를 기다리며 `send()`에서 막힙니다. `max.block.ms`는 따로 설정하지 않아 Kafka 기본값 60000ms입니다.

### 기대 증상

| 출처 | 기대 값 |
|---|---|
| saga_state | `STARTED` → `RESERVED`(약 3초) → `TIMED_OUT`(스위퍼) |
| payment_records | 두 주문 모두 `SUCCESS` |
| ledger_entries | 두 주문 모두 `<orderId>:PAID`, amount 10000 |
| payment outbox_events | `PAYMENT_COMPLETED`, topic `payment-complete`, `PENDING`이 주문 수만큼 쌓임 |
| order outbox_events | `ORDER_CREATED` PUBLISHED, 스위퍼가 적재한 `PAYMENT_FAILED` PUBLISHED |
| Kafka 토픽 목록 | `payment-complete`가 없음 |
| DLQ | 해당 주문 메시지 없음 |
| 지표(payment) | `outbox_pending` = 주문 수, `outbox_publish_failed_total`이 약 60초마다 +1, `outbox_published_total` 정체, `payment_processed_total{result="new"}` 증가 |
| 지표(order) | `saga_swept_total` +주문 수, `saga_dlq_recovered_total` 변화 없음 |
| health | payment-service `UP`(프로세스는 정상) |
| 로그(payment) | `NetworkClient : ... Error while fetching metadata with correlation id N : {payment-complete=UNKNOWN_TOPIC_OR_PARTITION}` 반복 / `LoggingProducerListener`의 스택트레이스에 `org.apache.kafka.common.errors.TimeoutException: Topic payment-complete not present in metadata after 60000 ms.` / `OutboxDispatcher : Failed to publish outbox event: id=<outboxId>, error=Send failed` |
| 로그(order) | `Saga sweeper: found 2 stuck saga(s) older than 60000ms`, `Saga timed out and compensated: sagaId=<orderId>, lastStatus=RESERVED` |

`OutboxDispatcher`의 에러 로그에는 `Send failed`만 남고 실제 원인 문구는 `LoggingProducerListener` 쪽에 있습니다. 로그 도구가 ERROR 한 줄만 보면 원인을 놓칩니다.

### 정답 원인
payment-service의 `app.kafka.topics.payment-completed` 값이 존재하지 않는 토픽(`payment-complete`)으로 잘못 설정돼, 결제 완료 이벤트가 Outbox에서 발행되지 못하고 쌓였다. 사가 타임아웃은 그 결과다.

### 원인 판정에 필요한 증거
- 필수: payment outbox에 `status=PENDING`, `topic=payment-complete` 행이 쌓여 있고, 정상 토픽 이름(`payment-completed`)과 다름.
- 필수: 결제 자체는 커밋됨(`payment_records` SUCCESS, `ledger_entries` PAID). S1과 구분하는 핵심.
- 필수: `UNKNOWN_TOPIC_OR_PARTITION` 또는 `not present in metadata after 60000 ms` 로그.
- 보조: payment `outbox_pending` 증가와 `outbox_publish_failed_total` 증가, payment-service health는 UP.

### 혼동하기 쉬운 오답
- "payment-service 다운"(S1과 사가 상태가 같음. health UP과 결제 커밋 흔적으로 구분).
- "Kafka 브로커 장애"(다른 토픽 발행·소비는 정상. order-service outbox는 PUBLISHED).

### 복구 시 주의
Outbox 행은 적재 시점의 토픽 이름을 `topic` 컬럼에 저장합니다. 설정만 고쳐 재기동하면 이미 쌓인 행은 여전히 `payment-complete`로 재시도돼 계속 막힙니다. 스크립트는 payment-service를 내린 상태에서 PENDING 행의 `topic`을 고친 뒤 다시 띄웁니다. 이렇게 복구하면 payment-completed가 늦게 도착하지만 사가는 이미 `TIMED_OUT`이라 무시되고, S1과 같은 "원장상 결제됨 + 주문 취소" 불일치가 남습니다.

또한 발행이 막힌 동안 payment-service 종료가 최대 `max.block.ms`만큼 늦어지고, 종료 중에는 `error=Producer closed while send in progress` 로그가 남습니다.

## 6. S4 재고 부족(대조군)

스크립트: `scripts/incidents/s4-insufficient-stock.sh`

### 주입 방법
1. 새 상품의 재고를 1로 초기화합니다(`POST /inventory/{productId}/init?stock=1`).
2. 같은 상품으로 수량 5 주문을 넣습니다.

### 기대 증상

| 출처 | 기대 값 |
|---|---|
| saga_state | `STARTED` → `FAILED_INSUFFICIENT_STOCK`(약 1초) |
| orders | `CANCELED` |
| order outbox_events | `ORDER_CREATED` 1건만(PUBLISHED). 보상 이벤트 없음 |
| payment DB | 해당 주문 흔적 없음 |
| 재고 | 1 그대로 |
| DLQ | 해당 주문 메시지 없음 |
| 지표 | `inventory_reserve_total{result="insufficient_stock"}` +1, `saga_count{status="FAILED_INSUFFICIENT_STOCK"}` +1, `saga_swept_total`·`saga_dlq_recovered_total` 변화 없음 |
| 로그(inventory) | `RedisInventoryStore : Insufficient stock: productId=<productId>, requested=5` (WARN) |
| 로그(order) | `PaymentResultEventListener : Inventory failed: orderId=<orderId>, reason=Insufficient stock` |

### 정답 원인
장애가 아니다. 요청 수량이 재고보다 많아 inventory-service가 정상적으로 예약을 거절했고, 사가가 설계대로 종결됐다.

### 원인 판정에 필요한 증거
- 필수: 사가 종착이 `FAILED_INSUFFICIENT_STOCK`.
- 필수: 장애 신호가 없음(DLQ 없음, `saga_swept_total`·`saga_dlq_recovered_total` 증가 없음, 모든 서비스 UP, Outbox PENDING 없음).
- 보조: `Insufficient stock` 로그의 requested 값과 재고 조회 결과.

### 혼동하기 쉬운 오답
- 원인 후보를 "장애"로 내는 것 자체가 오답입니다(예: "inventory-service 오류", "재고 데이터 손상").
- WARN 로그 한 줄을 근거로 높은 확신도의 장애를 보고하는 것.

## 7. 채점 기준

리포트 JSON의 원인 후보 1순위를 기준으로 채점합니다. 판정 로직은 `scripts/incidents/grade.py`에 있고, 오프라인 테스트는 `python3 -m unittest scripts/incidents/test_grade.py`로 돌립니다. `diagnose.sh`를 실행할 때마다 채점 요약이 `build/incidents/reports/scores.jsonl`에 한 줄씩 쌓여 적중률을 집계할 수 있습니다.

### 시나리오별 판정 값

리포트의 `component`와 `category`는 `diagnose.sh`가 넘기는 JSON Schema의 enum 값입니다. 1순위 후보가 아래 값이고 `is_incident`도 맞으면 적중입니다.

| ID | is_incident | component | category | 필수 증거 출처(`evidence.source`) | 혼동 표시 |
|---|---|---|---|---|---|
| S1 | true | payment-service | service_down | health, db | payment-service/config_error: S3와 혼동 |
| S2 | true | order-service 또는 inventory-service | poison_message | dlq | inventory-service/business_failure: S4와 혼동, inventory-service/code_defect: 부수 증상(보상 IOOBE)을 원인으로 봄 |
| S3 | true | payment-service | config_error | db, logs | payment-service/service_down: S1과 혼동, kafka/broker_issue: 브로커 장애로 오판 |
| S4 | false | inventory-service 또는 none | business_failure | db | 장애로 보고하면 오탐 |

S2는 깨진 payload를 만든 쪽(order-service Outbox)과 처리하지 못한 쪽(inventory-service)을 모두 정답으로 인정합니다. 확신도는 적중했는데 0.5 미만이면 "확신도 낮음", 오답인데 0.7 이상이면 "과신"으로 표시합니다. S4에서 장애를 0.7 이상으로 보고하면 "최대 감점"으로 따로 표시합니다.

### 항목별 기준

| 항목 | 기준 |
|---|---|
| 원인 적중 | 1순위 원인이 위 "정답 원인"과 같은 대상·같은 메커니즘을 가리키면 적중. S1은 payment-service 중단, S2는 order-created payload 역직렬화 실패(poison), S3는 payment-completed 토픽 이름 오설정, S4는 "장애 아님" |
| 근거 | 각 시나리오 "필수" 증거 중 리포트가 실제 도구 조회 결과로 인용한 비율 |
| 구분력 | S1과 S3, S2와 S4를 서로 혼동하지 않았는지 |
| 확신도 | 적중했는데 확신도가 낮거나, 오답인데 확신도가 높으면 감점. S4에서 장애를 높은 확신도로 보고하면 가장 크게 감점 |

## 8. 검증 중 발견한 결함(후속 과제)

시나리오를 돌리다 나온 문제입니다. 이번 단계에서는 코드를 고치지 않고 기록만 합니다.

1. `RedisInventoryStore.compensate()`가 예약이 없을 때 no-op가 아니라 예외를 던집니다. `COMPENSATE_SCRIPT`가 nil을 돌려주면 결과가 `null`이 아니라 원소 1개(`[null]`)인 리스트로 와서 `result.get(1)`에서 `IndexOutOfBoundsException: Index 1 out of bounds for length 1`이 납니다. 예약 전에 실패한 사가(S2)나 보상 이벤트 재전달 시 보상이 DLQ로 갑니다. 주석이 말하는 "예약 없으면 no-op" 동작이 Redis 백엔드에서는 성립하지 않습니다.
2. `payment-failed.DLQ`를 선언한 `NewTopic` 빈이 없습니다. auto-create=false 브로커에서는 inventory-service의 DLQ 발행이 60초 뒤 실패하고, `DefaultErrorHandler`가 레코드를 다시 시도해 payment-failed 파티션 소비가 무한히 막힙니다(검증 중 실제로 관찰). auto-create=true면 토픽이 생기지만 구독자가 없어 격리된 보상이 방치됩니다.
3. 타임아웃으로 종결된 사가에 결제가 늦게 성공하면(S1 복구, S3 복구) 원장에는 PAID가 남고 주문은 취소·재고 복원 상태가 됩니다. 환불(역분개) 경로가 없습니다.
4. Outbox 행이 토픽 이름을 저장하므로 토픽 설정 오류는 설정 수정만으로 복구되지 않습니다(S3 복구 주의).

## 9. 검증 기록

2026-11-15, 1절의 로컬 스택(맥 8GB, OrbStack 도커)에서 각 스크립트를 실행했습니다.

| ID | 결과 | 관찰 값 |
|---|---|---|
| S1 | 검증됨, 체크 10개 통과 | RESERVED까지 2s, TIMED_OUT까지 63s, `saga_swept_total` 0→1, `inventory_compensate_total{restored}` 0→1. 복구 후 `payment_records` SUCCESS 생성, 사가는 TIMED_OUT 유지 |
| S2 | 검증됨, 체크 11개 통과 | Outbox PUBLISHED까지 3s, FAILED_POISON까지 추가 4s, `saga_dlq_recovered_total` +1, `saga_swept_total` 변화 없음, payment-failed.DLQ에 IndexOutOfBoundsException 레코드 |
| S3 | 검증됨, 체크 11개 통과 | 주문 2건, payment outbox PENDING 2, 첫 발행 실패까지 60s(`outbox_publish_failed_total` 0→1), 사가 2건 TIMED_OUT, 복구 후 PENDING 0 |
| S4 | 검증됨, 체크 11개 통과 | FAILED_INSUFFICIENT_STOCK까지 3s, `inventory_reserve_total{insufficient_stock}` 0→1, 재고 1 유지 |

### 미검증
- k3s(OrbStack Kubernetes) 배포 환경 전체. 이번에는 클러스터를 띄우지 않았습니다. k3s에서는 서비스 중지가 `kubectl scale`, 설정 덮어쓰기가 Deployment env, 로그가 `kubectl logs`로 바뀌고 스크립트는 그대로 쓸 수 없습니다.
- 브로커 auto-create=true에서의 S3. 코드상으로는 오타 토픽이 자동 생성돼 발행이 성공하고, 구독자가 없어 사가가 RESERVED에서 멈춘 뒤 TIMED_OUT될 것으로 보이지만 돌려 보지 않았습니다.
- Prometheus 서버를 통한 지표 조회. 이번에는 `/actuator/prometheus`를 직접 읽었습니다.
