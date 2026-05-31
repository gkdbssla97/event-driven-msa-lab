# CLAUDE.md — event-driven-msa-lab

## AI 행동 지침
- 모든 구현 요청에 `karpathy-guidelines` 스킬을 적용한다.

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
| `websocket-service` | — | 미사용 (스캐폴딩만 존재) |

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

### Step 2 — Outbox Pattern (PR #3, feature/step2-outbox-pattern 브랜치, 미머지)
- `OutboxEvent` JPA 엔티티: id, aggregateId, eventType, payload(TEXT), status(PENDING/PUBLISHED), createdAt, publishedAt
- `OutboxRepository.findByStatusOrderByCreatedAtAsc()`
- `OrderService.create()`: Order + OutboxEvent를 같은 @Transactional에서 저장
- `OutboxPublisher`: @Scheduled(1000ms) 폴링 → Kafka 발행 → PUBLISHED 마킹
- `OrderEventPublisher` 삭제 (직접 발행 방식 제거)
- producer value-serializer → StringSerializer (payload가 이미 직렬화된 JSON string)

## 다음 단계 (예정)

### Step 3 — DLQ + k3s 운영 설정
- DLQ(Dead Letter Queue) 설정: 재시도 소진 후 별도 토픽으로 격리
- k3s: Deployment, Service, ConfigMap, Secret, Liveness/Readiness Probe 매니페스트
- 로그 수집 또는 메트릭 노출 고려

### AI 툴 추가 (Step 3 이후)
- **이벤트 컨트랙트 변경 감지기** (우선순위 높음): event-contracts 변경 시 영향받는 컨슈머 목록 자동 탐지
- CHANGELOG 자동 생성기
- 테스트 커버리지 분석기

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
- PR #3 (`feature/step2-outbox-pattern`): 리뷰 대기 중 / 머지 전
  - 머지 후 `feature/step3-dlq-k3s` 브랜치에서 Step 3 시작
