---
applyTo: "**/*Test.java,**/*Tests.java,event-contracts/**/*.java,order-service/**/*.java,payment-service/**/*.java,websocket-service/**/*.java"
---

# Testing Conventions

이 저장소의 테스트는 관찰 가능한 결과를 검증해야 한다.

## Test Strategy

- `event-contracts` 는 계약 생성과 직렬화 같은 작은 단위 테스트를 우선한다.
- HTTP 진입점은 MockMvc 또는 실제 응답 검증 형태를 우선한다.
- 메시징 흐름은 Embedded Kafka 로 검증한다.
- WebSocket relay 는 destination 과 payload 전달 여부를 검증한다.

## Expectations

- 구현과 테스트는 가능한 한 같은 변경 흐름 안에서 함께 추가한다.
- 테스트는 실제 상태 변화, 발행 이벤트, 응답 코드, 브로드캐스트 결과를 확인한다.
- 단순히 contextLoads 만 남기는 방향으로 후퇴하지 않는다.

## Verification

- Java/Spring 관련 변경 뒤에는 `./gradlew test` 또는 `./gradlew build` 기준 검증을 우선한다.
