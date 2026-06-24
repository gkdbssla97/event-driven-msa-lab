# Deep Dive 학습 가이드

> 이 프로젝트를 **내 것으로 만들기 위한** 단계별 학습 가이드.
> AI가 코드를 짰더라도, 코드를 읽고 이해하고 직접 실험하면 그건 내 지식이 됩니다.

## 학습 철학

```
코드를 읽는다 → "왜 이렇게?" 질문한다 → 직접 바꿔본다 → 깨진 걸 고친다 → 이해한다
```

**절대 하지 말 것**: 한번에 전부 이해하려 하기
**반드시 할 것**: 한 파일씩, 한 흐름씩, 직접 로컬에서 돌려보며 확인

---

## Phase 1: 전체 그림 잡기 (1~2시간)

### 1.1 서비스가 뭐가 있는지 파악

터미널에서 직접 확인:
```bash
ls -d */             # 최상위 모듈 확인
cat settings.gradle.kts  # Gradle이 어떤 모듈을 포함하는지
```

**확인할 것**: 4개 모듈이 있다 — `event-contracts`, `order-service`, `payment-service`, `websocket-service`

### 1.2 "주문 하나가 생기면 어떤 일이 벌어지는가?"

이 프로젝트의 **핵심 흐름**을 종이나 노트에 직접 그려보세요:

```
POST /orders {"userId":"user-1"}
    │
    ▼
  order-service: Order 저장 + OutboxEvent 저장 (같은 트랜잭션!)
    │
    ▼ (1초마다 폴링)
  OutboxPublisher → Kafka "order-created" 토픽에 발행
    │
    ▼
  payment-service: 이벤트 수신 → PaymentRecord 저장 → "payment-completed" 발행
    │
    ├──▶ order-service: 주문 상태 PENDING → CONFIRMED
    └──▶ websocket-service: WebSocket으로 브라우저에 실시간 알림
```

### 1.3 직접 돌려보기 (docker-compose)

K8s 없이 로컬에서 먼저 돌려보세요:
```bash
# Kafka 띄우기
docker-compose up -d

# 3개 서비스 각각 별도 터미널에서
./gradlew :order-service:bootRun
./gradlew :payment-service:bootRun
./gradlew :websocket-service:bootRun

# 주문 생성
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1"}'
```

**관찰할 것**: 3개 터미널의 로그를 보면서 이벤트가 어떻게 흘러가는지 확인

---

## Phase 2: 서비스별 코드 읽기 (각 1~2시간)

### 2.1 event-contracts — 공유 이벤트 정의

**읽을 파일** (4개, 매우 짧음):
```
event-contracts/src/main/java/.../contracts/
├── DomainEvent.java          ← sealed interface (모든 이벤트의 부모)
├── OrderCreatedEvent.java    ← record (불변 객체)
├── PaymentCompletedEvent.java
└── PaymentFailedEvent.java
```

**핵심 질문**:
- Q: 왜 별도 모듈로 분리했을까?
  → 3개 서비스가 **같은 이벤트 클래스**를 공유해야 하니까. 각 서비스가 event-contracts를 dependency로 가져다 씀
- Q: 왜 `record`를 사용했을까?
  → 이벤트는 불변(immutable)이어야 하고, record는 `equals`, `hashCode`, `toString`을 자동 생성
- Q: `sealed interface DomainEvent`의 의미는?
  → 이 프로젝트에서 허용되는 이벤트 타입을 컴파일 타임에 제한

**실험**:
- `OrderCreatedEvent`에 `amount` 필드를 추가하면 어떻게 될까?
- 빌드 → 어떤 서비스에서 깨지는지 확인

---

### 2.2 order-service — Outbox Pattern의 핵심

**읽는 순서** (중요!):

#### Step A: 엔티티부터
```
Order.java          ← 주문 엔티티. status 필드에 주목 (PENDING → CONFIRMED/CANCELED)
OrderStatus.java    ← enum: PENDING, CONFIRMED, CANCELED
OutboxEvent.java    ← Outbox 이벤트 엔티티. status(PENDING/PUBLISHED), payload(JSON 문자열)
OutboxStatus.java   ← enum: PENDING, PUBLISHED
```

**핵심 질문**:
- Q: `OutboxEvent.pending()` 팩토리 메서드는 왜 static인가?
  → 생성자 대신 의미있는 이름으로 객체 생성 (정적 팩토리 패턴)
- Q: `payload`가 왜 `String`(JSON)이지 객체가 아닌가?
  → DB TEXT 컬럼에 저장해야 하고, Kafka에도 문자열로 전송하니까

#### Step B: 서비스 로직
```
OrderService.java   ← create() 메서드를 집중해서 읽기
```

**가장 중요한 코드** — `create()` 메서드:
```java
@Transactional  // ← 이 하나의 트랜잭션 안에서:
public OrderCreateResponse create(OrderCreateRequest request) {
    Order order = Order.create(orderId, request.userId());
    orderRepository.save(order);                            // 1. 주문 저장

    OutboxEvent outbox = OutboxEvent.pending(...);
    outboxRepository.save(outbox);                          // 2. 이벤트도 저장

    return new OrderCreateResponse(...);
}
```

**핵심 질문**:
- Q: **왜 Kafka로 바로 보내지 않고 DB에 먼저 저장하는가?** (이게 Outbox Pattern의 핵심!)
  → DB 트랜잭션은 원자적(atomic). Order 저장과 이벤트 저장이 **둘 다 성공하거나 둘 다 실패**
  → 만약 Kafka로 직접 보내면? Order는 저장됐는데 Kafka 전송이 실패하면 **이벤트 유실**
  → Outbox Pattern은 "일단 DB에 확실히 저장 → 나중에 따로 Kafka로 전송"

#### Step C: Outbox 발행자
```
OutboxPublisher.java  ← @Scheduled로 1초마다 폴링
```

**핵심 질문**:
- Q: `@Scheduled(fixedDelay=1000)`은 무엇?
  → 1초마다 PENDING 상태인 OutboxEvent를 찾아서 Kafka로 전송
- Q: `@SchedulerLock`은 왜 필요?
  → 서버가 3대면 3개의 OutboxPublisher가 동시에 돌아감 → 같은 이벤트를 3번 전송할 수 있음
  → ShedLock이 "이 시점에 1대만 실행"을 DB 락으로 보장
- Q: 전송 성공 후 `event.markPublished()`는?
  → PENDING → PUBLISHED로 바꿔서 다음 폴링에서 다시 전송하지 않도록

#### Step D: 결과 수신
```
PaymentResultEventListener.java  ← payment-completed/failed 수신
```

**핵심**: order-service는 이벤트를 **보내기만 하는 게 아니라 받기도 함**
- `payment-completed` → `orderService.confirm()` → 주문 상태 CONFIRMED
- `payment-failed` → `orderService.cancel()` → 주문 상태 CANCELED

**실험**:
1. `OutboxPublisher`의 `fixedDelay`를 10000(10초)으로 바꾸고 주문 생성 → 실제로 10초 후에 이벤트가 발행되는지 확인
2. `@SchedulerLock` 어노테이션을 주석 처리하고 replicas=2로 배포 → 중복 이벤트 관찰

---

### 2.3 payment-service — 이벤트 소비 + 멱등성

**읽는 순서**:

```
OrderCreatedEventListener.java   ← Kafka consumer: 이벤트 수신 진입점
PaymentService.java              ← 결제 처리 로직 (멱등성!)
PaymentRecord.java               ← 결제 기록 엔티티
PaymentCompletedEventPublisher.java  ← 결제 완료 이벤트 발행
PaymentFailedEventPublisher.java     ← 결제 실패 이벤트 발행
```

**핵심 질문**:
- Q: `PaymentService.process()`에서 `existsById` 체크는 왜?
  → **멱등성(Idempotency)**. 같은 orderId로 2번 호출되면 2번째는 무시
  → Kafka는 at-least-once 보장 → 같은 메시지가 **재전송될 수 있음**
  → DB에 이미 있으면 "이미 처리됨" → 중복 결제 방지

- Q: `OrderCreatedEventListener`의 retry 로직은?
  → `processWithRetry()`가 최대 3번 재시도, 실패하면 `payment-failed` 이벤트 발행
  → 일시적 장애(DB 일시 불가 등)는 재시도로 복구 가능

- Q: `KafkaConsumerConfig.java`는 뭐하는 파일?
  → DLQ(Dead Letter Queue) 설정. 재시도 소진 후에도 실패하면 `.DLQ` 토픽으로 메시지 이동

**실험**:
1. `PaymentService.process()` 안에 `throw new RuntimeException("강제 실패")`를 넣고 → 3번 재시도 후 `payment-failed` 이벤트 발행되는지 확인
2. 같은 orderId로 curl을 2번 보내면 어떻게 되는지 확인 (Outbox에 2개 저장 → payment-service에서 2번째는 skip)

---

### 2.4 websocket-service — 실시간 알림 브릿지

**읽는 순서**:

```
WebSocketConfig.java                ← STOMP 설정
PaymentCompletedEventListener.java  ← Kafka → WebSocket 브릿지
PaymentFailedEventListener.java
PaymentUpdateBroadcaster.java       ← 실제 WebSocket 전송
```

**핵심 질문**:
- Q: 이 서비스는 DB가 없다. 왜?
  → 저장할 게 없음. Kafka 이벤트를 받아서 WebSocket으로 **중계(relay)만** 함
- Q: `/topic/orders/{orderId}`는 무엇?
  → STOMP 구독 경로. 브라우저가 특정 주문을 구독하면, 그 주문의 결제 결과만 실시간 수신

**실험**:
1. `http://localhost:8083/index.html` 브라우저에서 열기
2. orderId 입력해서 구독
3. 다른 터미널에서 해당 주문 생성 → 브라우저에 실시간 알림 확인

---

## Phase 3: 인프라 이해하기 (1~2시간)

### 3.1 Kafka 설정

**읽을 파일**: `infra/kafka/values-dev.yaml`

**핵심 질문**:
- Q: KRaft란? ZooKeeper와 뭐가 다른가?
  → Kafka 4.0부터 ZooKeeper 없이 자체 메타데이터 관리 (더 간단한 운영)
- Q: `replication.factor: "1"`은 왜?
  → 브로커 1대인 로컬에서 브로커 수 >= replication.factor 조건 필요

### 3.2 K8s 매니페스트

**읽을 파일**: `infra/k3s/apps/*.yaml` (3개)

**핵심 질문**:
- Q: `livenessProbe`와 `readinessProbe`의 차이?
  → liveness 실패 → 컨테이너 **재시작** (죽은 거 살리기)
  → readiness 실패 → 트래픽 **차단** (아직 준비 안 된 거 보호)
- Q: `initialDelaySeconds: 60`은 왜 이렇게 큰가?
  → Spring Boot 기동에 ~30초 + 여유분. 이거보다 작으면 기동 중에 kill당함
- Q: `ConfigMap`의 `KAFKA_BOOTSTRAP_SERVERS`는?
  → K8s 내부에서 Kafka 접근 주소. 환경변수로 주입

**실험**:
1. `initialDelaySeconds: 5`로 바꾸고 배포 → CrashLoopBackOff 직접 체험
2. `readinessProbe`를 제거하고 배포 → 기동 중에 트래픽이 들어오면 어떻게 되는지 관찰

---

## Phase 4: 의도적으로 깨뜨려보기 (가장 중요!)

이해한 것을 **확신**으로 만드는 단계. 일부러 문제를 만들고, 왜 그렇게 되는지 설명할 수 있으면 이해한 겁니다.

### 실험 1: Outbox 없이 직접 Kafka 전송
`OrderService.create()`에서 OutboxEvent 저장 대신 직접 `kafkaTemplate.send()` 호출.
→ Kafka가 죽어있을 때 주문 생성하면? → 이벤트 유실!

### 실험 2: 멱등성 제거
`PaymentService.process()`에서 `existsById` 체크 삭제.
→ 같은 메시지가 2번 오면? → 중복 결제 레코드!

### 실험 3: ShedLock 없이 스케일아웃
`@SchedulerLock` 제거 + replicas=2.
→ 같은 이벤트가 2번 발행되는지 로그로 확인

### 실험 4: DLQ 동작 확인
payment-service에서 항상 예외 throw.
→ DLQ 토픽에 메시지가 쌓이는지 확인

---

## Phase 5: 나만의 변경 만들기

여기까지 왔으면, 직접 기능을 추가해보세요:

### 쉬움
- [ ] 주문 조회 API: `GET /orders/{orderId}` → 현재 상태 반환
- [ ] 주문 목록 API: `GET /orders` → 전체 주문 리스트

### 보통
- [ ] 결제 금액 추가: `OrderCreatedEvent`에 `amount` 필드 추가 → 전 서비스 반영
- [ ] 결제 실패 시나리오: 특정 userId면 결제 실패하도록 로직 추가

### 어려움
- [ ] Outbox 재시도 로직: PUBLISHED 실패한 이벤트를 N분 후 재시도
- [ ] 이벤트 순서 보장: 같은 orderId는 같은 Kafka 파티션으로 (이미 되어있음 — 확인해보세요)

---

## 학습 체크리스트

각 항목을 **말로 설명할 수 있으면** 체크:

- [ ] "POST /orders 호출 시 order-service 내부에서 일어나는 일"을 설명할 수 있다
- [ ] "Outbox Pattern을 왜 쓰는지, 안 쓰면 뭐가 문제인지" 설명할 수 있다
- [ ] "OutboxPublisher가 하는 일"을 설명할 수 있다
- [ ] "ShedLock이 왜 필요한지" 설명할 수 있다
- [ ] "payment-service의 멱등성이 왜 필요한지" 설명할 수 있다
- [ ] "DLQ는 무엇이고 언제 사용되는지" 설명할 수 있다
- [ ] "liveness vs readiness probe 차이"를 설명할 수 있다
- [ ] "event-contracts 모듈을 분리한 이유"를 설명할 수 있다
- [ ] "Kafka consumer group이 무엇이고 왜 필요한지" 설명할 수 있다
- [ ] "WebSocket STOMP 브로드캐스트 흐름"을 설명할 수 있다
