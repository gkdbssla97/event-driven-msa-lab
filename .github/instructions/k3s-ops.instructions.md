---
applyTo: "infra/**/*.md,infra/**/*.yaml,scripts/dev/**/*.sh,Makefile,README.md"
---

# k3s / Helm Operations Conventions

이 저장소의 인프라 자산은 로컬 k3s + Helm 기반 개발 흐름을 기준으로 유지한다.

## Scope

- 인프라 자산은 `infra/` 와 `scripts/dev/` 아래에서 관리한다.
- 현재 범위는 single-node k3s 와 개발용 Kafka 배포 자산이다.
- host 에서 실행하는 애플리케이션 경로와 cluster 내부 Kafka 경로를 문서에서 구분한다.

## Scripts

- `make dev-up`, `make dev-status`, `make dev-topic-*` 흐름을 깨지 않게 유지한다.
- 스크립트는 선행 조건이 없을 때 조용히 망가지지 말고 이유를 설명하며 실패해야 한다.
- 로컬 개발용 값 파일은 `infra/kafka/values-dev.yaml` 기준으로 맞춘다.

## Diagrams and Docs

- k3s, Helm, Kafka 관계가 바뀌면 README 와 `docs/diagrams` 초안도 함께 갱신한다.
- README 는 GitHub 렌더링 친화적인 Mermaid 를, `docs/diagrams` 는 PlantUML 초안을 유지한다.
