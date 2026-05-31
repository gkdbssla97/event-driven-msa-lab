# Repository Copilot Instructions

이 저장소는 Kafka, Spring Boot, WebSocket, k3s, Helm 을 단계적으로 확장하는 실습형 모노레포다.

항상 아래 원칙을 먼저 따른다.

- 모듈 경계를 유지한다.
- `event-contracts` 에는 공유 계약만 둔다.
- 작은 수직 슬라이스 단위로 구현한다.
- 현재 Phase 를 넘어서는 복잡도를 미리 당겨오지 않는다.
- 테스트와 문서를 구현과 함께 유지한다.

작업 전 기본 확인 항목:

- `README.md` 의 현재 범위와 로드맵을 읽는다.
- 필요한 경우 `.github/instructions/*.instructions.md` 의 path-specific 규칙을 먼저 따른다.
- Java/Spring 변경은 build/test 검증 없이 완료로 간주하지 않는다.
- k3s/Helm 관련 작업은 현재 `make dev-*` 흐름과 `infra/` 자산을 기준으로 맞춘다.
