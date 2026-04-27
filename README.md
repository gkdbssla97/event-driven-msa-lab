# event-driven-msa-lab

Kafka, Kubernetes, Redis, and WebSocket based toy project for practicing event-driven architecture with small but realistic service boundaries.

## Project Goal

This repository is a single-repo, multi-module starter for an event-driven system built by a single developer. The goal is not CRUD complexity but learning practical distributed-system patterns:

- asynchronous event flow with Kafka
- loose coupling between services
- real-time client feedback through WebSocket
- production-style patterns such as outbox, retry, and idempotency

## Current Scope

This first scaffold intentionally stays small.

- Gradle multi-module monorepo
- three independently runnable Spring Boot services
- one lightweight shared module for event contracts
- README and structure for future Kafka flow

Not implemented yet:

- Kafka producer/consumer logic
- persistence and outbox tables
- Redis integration
- WebSocket handlers and subscriptions
- Kubernetes manifests and Helm values

## Module Structure

```text
event-driven-msa-lab
├── event-contracts
├── order-service
├── payment-service
└── websocket-service
```

### event-contracts

Shared event DTO and contract module. This module should stay small and should not contain service business logic.

### order-service

Future entry point for order creation APIs and `order-created` event publishing.

### payment-service

Future consumer for `order-created` events and producer for payment result events.

### websocket-service

Future real-time delivery layer that consumes domain events and pushes updates to clients.

## Target Event Flow

```text
Client
  -> Order Service
  -> Kafka: order-created
  -> Payment Service
  -> Kafka: payment-completed
  -> WebSocket Service
  -> Client real-time update
```

## Why This Structure

This project uses a monorepo because it keeps local development simple while preserving real runtime boundaries between services. Each service remains independently runnable, but contracts and infrastructure can still evolve together in one repository.

## Tech Baseline

- Java 17
- Gradle multi-module build
- Spring Boot 3.4.x
- JUnit 5

## Run Commands

From the repository root:

```bash
./gradlew projects
./gradlew test
./gradlew :order-service:bootRun
./gradlew :payment-service:bootRun
./gradlew :websocket-service:bootRun
```

## Roadmap

### Phase 1

- scaffold service boundaries
- define event contracts
- keep each service bootable

### Phase 2

- add Kafka topics and producer/consumer configuration
- connect order -> payment -> websocket flow

### Phase 3

- add persistence
- add outbox pattern
- add idempotency and retry handling

### Phase 4

- add Redis where it supports session state or replay protection
- add Kubernetes manifests or Helm values
- add observability and local operations scripts

## GitHub README Intent

If you publish this repository, this README should help reviewers understand that the project is intentionally staged: the current branch provides a clean architectural starting point, not the full event pipeline yet.
