---
applyTo: "**/*.java,**/*.kt,**/*.kts,**/*.yml,**/*.yaml,build.gradle.kts,settings.gradle.kts"
---

# Spring Boot / Java Conventions

이 저장소에서 AI 에이전트는 아래 규칙을 우선한다.

## Architecture

- 모듈 경계를 유지한다.
- `event-contracts` 에는 공유 계약만 둔다.
- 서비스 간 직접 호출보다 이벤트 흐름을 우선한다.
- 기능은 작은 수직 슬라이스 단위로 추가한다.

## Spring Boot

- 생성자 주입을 사용한다.
- 설정값은 `application.yml` 과 `@Value` 또는 타입 안전한 바인딩으로 관리한다.
- HTTP 계층, 서비스 계층, 메시징 계층의 역할을 분리한다.
- 요청 검증이 필요하면 Bean Validation 을 우선 사용한다.

## Kafka

- 토픽 이름은 각 서비스 설정에 둔다.
- producer/consumer 는 서비스 역할에 맞게 분리한다.
- 성공 여부가 중요한 publish 는 실패를 호출자에게 드러낸다.
- 이후 고도화 전까지는 단순한 이벤트 흐름을 유지한다.

## Testing

- 구현과 테스트를 함께 추가한다.
- 메시징 흐름은 가능하면 Embedded Kafka 로 검증한다.
- 테스트는 실제 관찰 가능한 결과를 검증한다.

## Style

- Java 17 기준으로 작성한다.
- 불필요한 추상화는 만들지 않는다.
- 타입 안전성을 해치지 않는다.
- 현재 저장소의 패키지/네이밍 규칙을 따른다.
