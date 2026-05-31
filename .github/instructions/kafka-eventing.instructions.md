---
applyTo: "event-contracts/**/*.java,order-service/**/*.java,payment-service/**/*.java,websocket-service/**/*.java,**/application.yml"
---

# Kafka Eventing Conventions

이 저장소에서 Kafka 관련 변경은 아래 규칙을 따른다.

## Boundaries

- `event-contracts` 는 공유 이벤트 계약만 가진다.
- 토픽 이름은 각 서비스 설정에서 관리한다.
- producer 와 consumer 책임을 같은 클래스에 과하게 섞지 않는다.

## Event Flow

- `order-service` 는 `order-created` 발행까지 책임진다.
- `payment-service` 는 `order-created` 소비 후 `payment-completed` 발행까지 책임진다.
- `websocket-service` 는 `payment-completed` 소비 후 실시간 릴레이까지만 책임진다.

## Delivery

- 성공 여부가 중요한 publish 는 실패를 호출자에게 드러낸다.
- 현재 Phase 에서는 단순한 이벤트 흐름을 우선한다.
- Outbox, DLQ, Retry, Idempotency 는 명시적 요청이 있을 때만 확장한다.

## Documentation

- 이벤트 이름, 토픽, 서비스 책임이 바뀌면 README 와 다이어그램도 함께 갱신한다.
