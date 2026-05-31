# CLAUDE.md — event-driven-msa-lab

## AI 행동 지침
- 모든 구현 요청에 `karpathy-guidelines` 스킬을 적용한다.

## 커밋 컨벤션
- **원자적 커밋**: 하나의 커밋은 하나의 논리적 변경만 포함한다.
- **프리픽스**: `docs`, `feat`, `fix`, `refactor` 사용.
- **상세 메시지**: 프리픽스 뒤 본문/설명은 한국어로 작성한다.
  - 예: `feat(step5): Actuator 헬스체크 + HTTP Probe 전환`

## 프로젝트 목적
이벤트 드리븐 MSA 패턴을 단계별로 직접 구현하며 학습하는 토이 프로젝트.
각 단계를 feature 브랜치에서 작업 → PR 생성(자동 코드 리뷰) → main 머지.

## 서비스 구조

```
POST /orders
    └─► order-service
            ├─ Order 저장 (H2)
            ├─ OutboxEvent 저장 (H2, 같은 트랜잭션)
            └─ OutboxPublisher(@Scheduled 1s)
                    └─► Kafka: order-created
                                └─► payment-service
                                        ├─ PaymentRecord 저장 (H2, 멱등성 보장)
                                        ├─► Kafka: payment-completed
                                        │           └─► order-service (CONFIRMED)
                                        └─► Kafka: payment-failed
                                                    └─► order-service (CANCELED)
```

## 모듈 구성

| 모듈 | 포트 | 역할 |
|------|------|------|
| `event-contracts` | — | 공유 이벤트 레코드 (OrderCreatedEvent, PaymentCompletedEvent, PaymentFailedEvent) |
| `order-service` | 8081 | 주문 생성/상태 관리, Outbox Pattern |
| `payment-service` | 8082 | 결제 처리, DB 기반 멱등성 |
| `websocket-service` | 8083 | 결제 결과 WebSocket STOMP 브로드캐스트 |

## 기술 스택
- Java 21 + Spring Boot 3, Spring Data JPA, Spring Kafka
- H2 in-memory DB (order-service, payment-service 각각)
- Kafka (로컬: docker-compose, 테스트: EmbeddedKafka)
- Gradle 멀티모듈

## 완료된 단계

### Step 1 — DB 영속성 + 전체 이벤트 플로우 (PR #2, merged)
- `Order` JPA 엔티티: PENDING → CONFIRMED / CANCELED
- `OrderRepository`, `OrderService.confirm()`, `OrderService.cancel()`
- `PaymentResultEventListener`: payment-completed/failed 수신 → 주문 상태 업데이트
- `PaymentRecord`: DB 기반 멱등성 (ConcurrentHashMap 대체)
- `PaymentFailedEventPublisher` + `PaymentFailedEvent`: 재시도 소진 시 발행

### Step 2 — Outbox Pattern (PR #3, merged)
- `OutboxEvent` JPA 엔티티: id, aggregateId, eventType, payload(TEXT), status(PENDING/PUBLISHED), createdAt, publishedAt
- `OutboxRepository.findByStatusOrderByCreatedAtAsc()`
- `OrderService.create()`: Order + OutboxEvent를 같은 @Transactional에서 저장
- `OutboxPublisher`: @Scheduled(1000ms) 폴링 → Kafka 발행 → PUBLISHED 마킹
- `OrderEventPublisher` 삭제 (직접 발행 방식 제거)
- producer value-serializer → StringSerializer (payload가 이미 직렬화된 JSON string)

### Step 3 — DLQ + k3s 운영 설정 (PR #4, merged)
- DLQ: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` → `order-created.DLQ` 토픽
- k3s 매니페스트: Deployment, Service, ConfigMap(KAFKA_BOOTSTRAP_SERVERS), tcpSocket Probe
- `KafkaTopicConfig`: NewTopic Bean으로 토픽 파티션 3개 선언적 관리
- order-service replicas=1 (Outbox poller 중복 방지, ShedLock 도입 전까지)
- payment-service/websocket-service replicas=3 (Kafka 파티션 1:1 매핑)
- Dockerfile: eclipse-temurin:21-jre 기반

### Step 4 — WebSocket 실시간 알림 (PR #5, merged)
- websocket-service: Spring WebSocket + STOMP 기반
- `PaymentCompletedEventListener` / `PaymentFailedEventListener`: Kafka → WebSocket 브릿지
- `PaymentUpdateBroadcaster`: `/topic/orders/{orderId}`로 결제 결과 브로드캐스트
- StringDeserializer + ObjectMapper 역직렬화 방식
- STOMP 브라우저 테스트 페이지 (`index.html`)

## 다음 단계 (예정)

### Step 5 — Spring Actuator + HTTP Probe + Ingress
- 3개 서비스에 `spring-boot-starter-actuator` 추가
- k3s Probe: tcpSocket → httpGet `/actuator/health` 전환
- Ingress 리소스: websocket-service 외부 접근 + WebSocket upgrade 지원

### Step 6 — ShedLock + order-service 스케일아웃
- ShedLock: DB 기반 분산 락으로 Outbox poller 중복 방지
- order-service replicas=1 → 3 스케일아웃

## 브랜치 전략
- `main`: 머지된 완성 코드
- `feature/step{N}-{description}`: 각 단계 작업 브랜치
- PR 머지 시 GitHub Actions AI 코드 리뷰 자동 실행

## 주요 설계 결정
- **Outbox payload는 StringSerializer**: OutboxEvent.payload가 이미 ObjectMapper로 직렬화된 JSON string이므로 Kafka로 그대로 전송
- **payment-service consumer**: StringDeserializer + objectMapper.readValue()로 역직렬화
- **H2 ddl-auto: create-drop**: 재시작 시 스키마 초기화 (테스트 환경용)
- **EmbeddedKafka topics**: order-created, payment-completed, payment-failed 3개 모두 명시 필요

## 현재 PR 상태
- PR #2 (Step 1): merged
- PR #3 (Step 2): merged
- PR #4 (Step 3): merged
- PR #5 (Step 4): merged
- 다음: `feature/step5-actuator-ingress` 브랜치에서 Step 5 시작
