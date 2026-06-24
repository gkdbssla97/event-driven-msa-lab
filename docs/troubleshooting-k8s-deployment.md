# K8s 로컬 배포 트러블슈팅 기록

> Step 5 (Actuator + HTTP Probe + Ingress) 작업 중 OrbStack Kubernetes 전환 과정에서 발생한 이슈 모음

## 목차
1. [k3d Agent Node NotReady](#1-k3d-agent-node-notready)
2. [CrashLoopBackOff — Liveness Probe 타임아웃](#2-crashloopbackoff--liveness-probe-타임아웃)
3. [Kafka Health Contributor 미존재 오류](#3-kafka-health-contributor-미존재-오류)
4. [payment-service Connection Refused — Tomcat 미기동](#4-payment-service-connection-refused--tomcat-미기동)
5. [Kafka __consumer_offsets 토픽 생성 실패](#5-kafka-__consumer_offsets-토픽-생성-실패)
6. [k3d → OrbStack 전환 결정](#6-k3d--orbstack-전환-결정)

---

## 1. k3d Agent Node NotReady

### 증상
```
$ kubectl get nodes
NAME                                STATUS     ROLES
k3d-event-driven-msa-lab-agent-0   NotReady   <none>
```

### 원인
디스크 정리(Docker prune) 과정에서 k3d의 agent 컨테이너가 영향을 받아 내부 kubelet이 정상 동작하지 못함.

### 해결
```bash
docker restart k3d-event-driven-msa-lab-agent-0
```

### 교훈
- `docker system prune`은 k3d 클러스터 노드 컨테이너에 영향을 줄 수 있음
- k3d 노드 이상 시 해당 Docker 컨테이너를 재시작하면 복구됨

---

## 2. CrashLoopBackOff — Liveness Probe 타임아웃

### 증상
```
State:          Waiting
  Reason:       CrashLoopBackOff
Last State:     Terminated
  Reason:       Error
  Exit Code:    143
```
파드가 READY 상태에 도달하지 못하고 반복 재시작.

### 원인
Spring Boot 기동 시간이 k3d 환경에서 **40~60초** 소요되는데, liveness probe의 `initialDelaySeconds`가 **30초**로 설정되어 있어 앱이 완전히 시작하기 전에 probe가 실패 → kubelet이 컨테이너를 kill.

### 해결
```yaml
# 변경 전
livenessProbe:
  initialDelaySeconds: 30
  failureThreshold: 3

# 변경 후
livenessProbe:
  initialDelaySeconds: 60
  failureThreshold: 5
readinessProbe:
  initialDelaySeconds: 30
  failureThreshold: 5
```

### 교훈
- 로컬 K8s 환경(리소스 제한)은 운영 대비 기동 시간이 2~3배 느림
- `initialDelaySeconds`는 **실제 관측된 기동 시간 + 여유** 기준으로 설정
- `failureThreshold`도 여유있게 설정해야 간헐적 지연에 대응 가능

---

## 3. Kafka Health Contributor 미존재 오류

### 증상
```
Included health contributor 'kafka' in group 'readiness' does not exist
  or does not match the expected type
```
Spring Boot 기동 자체가 실패 (ApplicationContextException).

### 원인
Spring Boot 3.4.5 + Spring Kafka 조합에서 `KafkaHealthIndicator`가 자동 등록되지 않음. `application.yml`의 readiness 그룹에 `kafka`를 포함했지만, 실제로 해당 health contributor bean이 존재하지 않아 기동 실패.

### 해결
3개 서비스 모두 `application.yml`에서 readiness 그룹 수정:
```yaml
# 변경 전
readiness:
  include: readinessState, db, kafka  # 또는 readinessState, kafka

# 변경 후
readiness:
  include: readinessState, db  # websocket-service는 readinessState만
```

### 교훈
- Spring Boot Actuator의 health contributor는 **실제 등록 여부를 확인**한 후 그룹에 포함해야 함
- `/actuator/health`로 실제 등록된 indicator 목록 확인 가능
- Kafka health indicator는 별도 설정이 필요하거나 Spring Boot 버전에 따라 미등록될 수 있음

---

## 4. payment-service Connection Refused — Tomcat 미기동

### 증상
```
Readiness probe failed: Get "http://10.42.0.x:8082/actuator/health/readiness":
  dial tcp 10.42.0.x:8082: connect: connection refused
```
로그에 `Started PaymentServiceApplication in X seconds`가 찍히지만, HTTP 포트가 열리지 않음.

### 원인
`payment-service/build.gradle.kts`에 `spring-boot-starter-web`이 없었음:
```kotlin
// 기존 (문제)
implementation("org.springframework.boot:spring-boot-starter")
implementation("org.springframework.boot:spring-boot-starter-json")
```
`spring-boot-starter`만으로는 **임베디드 Tomcat이 포함되지 않음** → HTTP 서버가 시작되지 않아 모든 HTTP probe가 connection refused.

### 해결
```kotlin
// 수정 후
implementation("org.springframework.boot:spring-boot-starter-web")
```

### 교훈
- `spring-boot-starter` ≠ `spring-boot-starter-web`. 전자는 HTTP 서버를 포함하지 않음
- Actuator endpoint도 `spring-boot-starter-web`이 있어야 HTTP로 노출됨
- "Started in X seconds" 로그만으로 HTTP 서버 정상 기동을 판단하면 안 됨
- **가장 위험한 유형**: 앱은 정상 시작되지만 HTTP 포트가 안 열려서, probe만 실패하는 패턴

---

## 5. Kafka __consumer_offsets 토픽 생성 실패

### 증상
```
[GroupCoordinator]: The group coordinator is not available.
```
Consumer가 그룹에 참여하지 못하고, 메시지를 소비하지 못함.

### 원인
`__consumer_offsets` 내부 토픽의 기본 `replication.factor`가 **3**이지만, 로컬에는 **브로커 1대**만 운영 중. 브로커 수 < replication.factor이면 토픽 생성 자체가 불가.

### 시행착오
| 시도 | 결과 |
|------|------|
| `controller.extraConfig` | bitnami chart에 해당 키 없음 |
| 최상위 `extraConfig` | 유효하지 않은 키 |
| `overrideConfiguration` (YAML `\|` 문자열) | JSON unmarshal 에러 |
| **`overrideConfiguration` (map 형식)** | **성공** |

### 해결
```yaml
# infra/kafka/values-dev.yaml
overrideConfiguration:
  offsets.topic.replication.factor: "1"
  transaction.state.log.replication.factor: "1"
  transaction.state.log.min.isr: "1"
```

### 교훈
- 단일 브로커 환경에서는 내부 토픽의 replication factor를 반드시 1로 설정
- bitnami/kafka Helm chart의 `overrideConfiguration`은 **map 형식만** 지원 (YAML multiline string 불가)
- `__consumer_offsets` 토픽이 없으면 **모든 consumer group이 작동하지 않음** → 이벤트 플로우 전체 중단

---

## 6. k3d → OrbStack 전환 결정

### 배경
k3d on Docker Desktop 환경에서 다음 병목 발생:
- `k3d image import`: 3개 서비스 이미지 로드에 **10~15분**
- Spring Boot 기동: 리소스 경합으로 **40~60초**
- 전체 배포 사이클: **20분+** (이미지 빌드 → import → 파드 기동)

### OrbStack 장점
| 항목 | k3d (Docker Desktop) | OrbStack |
|------|---------------------|----------|
| 이미지 전달 | `k3d image import` 필수 | **불필요** (host 이미지 직접 참조) |
| Spring Boot 기동 | 40~60초 | **17~27초** |
| 메모리 관리 | Docker Desktop 고정 할당 | **동적 할당** |
| 파드 안정성 | CrashLoopBackOff 빈발 | **재시작 0회** |
| 전체 사이클 | 20분+ | **~3분** |

### 전환 방법
```bash
# 1. OrbStack 설치
brew install orbstack  # 또는 공식 사이트에서 직접 설치

# 2. Kubernetes 활성화 (OrbStack 설정 → Kubernetes → Enable)

# 3. kubectl context 전환
kubectl config use-context orbstack

# 4. 기존 k3d 클러스터 제거 (선택)
k3d cluster delete event-driven-msa-lab

# 5. 그대로 배포 (이미지 import 불필요!)
kubectl apply -f infra/k3s/apps/
helm upgrade --install kafka bitnami/kafka -f infra/kafka/values-dev.yaml
```

### 주의사항
- OrbStack은 `imagePullPolicy: IfNotPresent` + 로컬 빌드 이미지를 자동 인식
- Docker Desktop과 동시 실행 시 충돌 가능 → Docker Desktop 종료 권장
- `brew install orbstack` postflight 스크립트가 SIGKILL되는 경우 있음 → 공식 사이트에서 직접 설치 권장
