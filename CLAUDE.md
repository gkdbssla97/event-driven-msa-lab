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
            └─ OutboxPublisher(@Scheduled 1s, ShedLock 분산 락)
                    └─► Kafka: order-created
                                └─► inventory-service (Redis, Lua 원자성)
                                        ├─ 재고 충분: inventory-reserved
                                        └─ 재고 부족: inventory-failed → order CANCELED
                                                └─► payment-service (DB 멱등성)
                                                        ├─ payment-completed → order CONFIRMED
                                                        └─ payment-failed
                                                                └─► inventory-service
                                                                      보상 트랜잭션(재고 복원)
```

## 모듈 구성

| 모듈 | 포트 | 역할 |
|------|------|------|
| `event-contracts` | — | 공유 이벤트 레코드 (OrderCreatedEvent, InventoryReservedEvent, InventoryFailedEvent, PaymentCompletedEvent, PaymentFailedEvent) |
| `order-service` | 8081 | 주문 생성/상태 관리, Outbox Pattern, ShedLock |
| `payment-service` | 8082 | 결제 처리, DB 기반 멱등성 |
| `websocket-service` | 8083 | 결제 결과 WebSocket STOMP 브로드캐스트 |
| `inventory-service` | 8084 | 재고 관리(Redis), Lua 원자성, 보상 트랜잭션, Redis SETNX 멱등성 |

## 기술 스택
- Java 21 + Spring Boot 3, Spring Data JPA, Spring Kafka, Spring Data Redis
- H2 in-memory DB (order-service, payment-service), Redis (inventory-service)
- Kafka (로컬: docker-compose, 테스트: EmbeddedKafka)
- Micrometer + Prometheus + Grafana (관측성)
- Gradle 멀티모듈, OrbStack(로컬 K8s)

## 완료된 단계

### Step 1 — DB 영속성 + 전체 이벤트 플로우 (PR #2, merged)
- `Order` JPA 엔티티: PENDING → CONFIRMED / CANCELED
- `PaymentRecord`: DB 기반 멱등성 (ConcurrentHashMap 대체)
- `PaymentFailedEventPublisher` + `PaymentFailedEvent`: 재시도 소진 시 발행

### Step 2 — Outbox Pattern (PR #3, merged)
- `OutboxEvent` JPA 엔티티: PENDING/PUBLISHED 상태 관리
- `OrderService.create()`: Order + OutboxEvent를 같은 @Transactional에서 저장
- `OutboxPublisher`: @Scheduled(1000ms) 폴링 → Kafka 발행 → PUBLISHED 마킹

### Step 3 — DLQ + k3s 운영 설정 (PR #4, merged)
- DLQ: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`
- k3s 매니페스트: Deployment, Service, ConfigMap, tcpSocket Probe → httpGet Probe
- Dockerfile: eclipse-temurin:21-jre 기반

### Step 4 — WebSocket 실시간 알림 (PR #5, merged)
- websocket-service: Spring WebSocket + STOMP 기반
- `PaymentUpdateBroadcaster`: `/topic/orders/{orderId}`로 결제 결과 브로드캐스트

### Step 5 — Spring Actuator + HTTP Probe + Ingress (PR #6, merged)
- 4개 서비스에 `spring-boot-starter-actuator` 추가
- k3s Probe: tcpSocket → httpGet `/actuator/health/liveness|readiness` 전환
- Ingress 리소스: websocket-service 외부 접근 + WebSocket upgrade 지원

### Step 6 — ShedLock + order-service 스케일아웃 (PR #7, merged)
- ShedLock: DB 기반 분산 락으로 Outbox poller 중복 방지
- `lockAtLeastFor=PT1S`, `lockAtMostFor=PT30S`
- `OutboxPublisherShedLockTest`: 가상 파드 2개가 동시에 호출해도 DB 조회 1회만 발생 검증

### Step 7 — Choreography Saga + inventory-service (PR #8,#9,#10, merged)
- inventory-service 신규: Redis 재고 관리, Lua 원자적 차감(RESERVE_SCRIPT), 보상 트랜잭션(COMPENSATE_SCRIPT)
- Kafka 흐름: order-created → inventory-reserved | inventory-failed → payment-completed | payment-failed
- 보상 트랜잭션 Lua 원자화: INCRBY + DEL을 단일 스크립트로 묶어 재전달 시 이중 복원 방지
- `InventoryServiceConcurrencyTest`: 50 스레드 동시 차감 → 오버셀 없음 검증
- k6 oversell-test: OrbStack 배포 환경에서 end-to-end 오버셀 방지 검증

### Step 8 — 멱등성 가드 + 관측성 (현재 브랜치: feature/step8-idempotency-observability)
- inventory-service `OrderCreatedEventListener`: Redis SETNX `claimProcessing()` 멱등성 가드 추가
- Micrometer 커스텀 메트릭: `inventory.reserve{result}`, `inventory.compensate{result}`, `outbox.pending`, `outbox.published`, `payment.processed{result}`
- `/actuator/prometheus` 엔드포인트 노출 (4개 서비스)
- K8s: `prometheus.io/scrape` 어노테이션으로 파드 자동 탐색
- `infra/k3s/monitoring/`: Prometheus + Grafana 배포 매니페스트 + 사전 구성 대시보드

## 브랜치 전략
- `main`: 머지된 완성 코드
- `feature/step{N}-{description}`: 각 단계 작업 브랜치
- PR 머지 시 GitHub Actions AI 코드 리뷰 자동 실행

## 주요 설계 결정
- **Outbox payload는 StringSerializer**: payload가 이미 ObjectMapper로 직렬화된 JSON string이므로 Kafka로 그대로 전송 (double-serialization 방지)
- **inventory-service 포트 8084**: websocket-service(8083)와 충돌 방지
- **Redis SETNX 멱등성**: inventory-service는 JPA 없이 Redis `setIfAbsent`로 중복 이벤트 처리 방지
- **Lua Script 원자성**: GET+DECRBY (reserve), GET+INCRBY+DEL (compensate) 각각 단일 스크립트로 race condition 원천 차단
- **ShedLock vs Lua 구분**: ShedLock = "어느 파드가 스케줄 메서드를 실행할 권한을 가지는가", Lua = "Redis 명령을 원자적으로 실행"

## 현재 PR 상태
- PR #2~#10: merged
- 현재 작업: `feature/step8-idempotency-observability`
