#!/usr/bin/env bash
# 장애 시나리오용 최소 로컬 스택. 메모리 8GB 맥에서도 돌도록 컨테이너 4개 + JVM 3개로 줄였다.
#
#   local-stack.sh up                      인프라 + 서비스 3개 기동
#   local-stack.sh down                    서비스 종료 + 컨테이너 정리(MySQL 볼륨은 유지)
#   local-stack.sh start|stop|restart <svc>   svc = order-service | payment-service | inventory-service
#   local-stack.sh status
#
# restart 시 현재 셸의 환경변수가 그대로 프로세스에 전달된다(S3가 토픽 이름을 바꿀 때 사용).
# 사전 조건: ./gradlew :order-service:bootJar :payment-service:bootJar :inventory-service:bootJar
#
# Kafka 는 auto.create.topics.enable=false 로 띄운다(KAFKA_AUTO_CREATE=true 로 바꿀 수 있음).
# 앱이 쓰는 토픽은 모두 NewTopic 빈으로 선언돼 있어 정상 흐름에는 영향이 없고,
# S3(토픽 이름 오설정)의 "메타데이터 대기 → Outbox PENDING 적체" 증상이 이 설정에서만 나온다.

source "$(dirname "$0")/lib.sh"

SERVICES=(order-service inventory-service payment-service)
JAVA_OPTS="${JAVA_OPTS:--Xmx256m -XX:+UseSerialGC -XX:TieredStopAtLevel=1}"

port_of() {
  case "$1" in
    order-service) echo 8081 ;;
    payment-service) echo 8082 ;;
    inventory-service) echo 8084 ;;
    *) printf 'unknown service: %s\n' "$1" >&2; exit 1 ;;
  esac
}

pid_file() { echo "$RUN_DIR/$1.pid"; }

start_infra() {
  log "MySQL(order, payment) 기동"
  (cd "$REPO_ROOT" && docker compose up -d mysql-order mysql-payment >/dev/null)

  if ! docker ps --format '{{.Names}}' | grep -qx edml-redis; then
    docker rm -f edml-redis >/dev/null 2>&1 || true
    log "Redis 기동"
    docker run -d --name edml-redis -p 6379:6379 redis:7-alpine >/dev/null
  fi

  if ! docker ps --format '{{.Names}}' | grep -qx "$KAFKA_CONTAINER"; then
    docker rm -f "$KAFKA_CONTAINER" >/dev/null 2>&1 || true
    log "Kafka(KRaft 단일 노드, auto.create.topics.enable=${KAFKA_AUTO_CREATE:-false}) 기동"
    docker run -d --name "$KAFKA_CONTAINER" -p 9092:9092 \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
      -e KAFKA_AUTO_CREATE_TOPICS_ENABLE="${KAFKA_AUTO_CREATE:-false}" \
      -e KAFKA_HEAP_OPTS="-Xmx384m -Xms384m" \
      "${KAFKA_IMAGE:-apache/kafka:latest}" >/dev/null
  fi

  wait_for 90 "MySQL order 준비" sql_order "SELECT 1" >/dev/null
  wait_for 90 "MySQL payment 준비" sql_payment "SELECT 1" >/dev/null
  wait_for 90 "Kafka 준비" kafka kafka-topics.sh --list >/dev/null 2>&1
  # inventory-service 의 DefaultErrorHandler 는 payment-failed 처리 실패를 payment-failed.DLQ 로 보내지만
  # 이 토픽을 선언하는 NewTopic 빈이 없다. auto-create=false 에서는 DLQ 발행이 max.block.ms 후 실패하고
  # 레코드가 무한 재시도되어 파티션이 막힌다. k3s 기본(auto-create=true)과 같은 상태로 맞추려고 미리 만든다.
  kafka kafka-topics.sh --create --if-not-exists --topic payment-failed.DLQ --partitions 3 --replication-factor 1 >/dev/null
}

start_service() {
  local svc="$1" port jar pf
  port="$(port_of "$svc")"
  jar="$REPO_ROOT/$svc/build/libs/$svc-0.0.1-SNAPSHOT.jar"
  pf="$(pid_file "$svc")"
  if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
    log "$svc 이미 실행 중(pid $(cat "$pf"))"
    return 0
  fi
  [[ -f "$jar" ]] || { log "jar 없음: $jar (bootJar 먼저 빌드)"; exit 1; }

  printf '\n===== %s start %s =====\n' "$svc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$LOG_DIR/$svc.log"
  # 스위퍼 타임아웃을 기본 5분/60초에서 줄여 S1·S3를 몇 분 안에 재현한다.
  SAGA_TIMEOUT_MS="$SAGA_TIMEOUT_MS" SAGA_SWEEP_INTERVAL_MS="$SAGA_SWEEP_INTERVAL_MS" \
    nohup java $JAVA_OPTS -jar "$jar" >> "$LOG_DIR/$svc.log" 2>&1 &
  echo $! > "$pf"
  wait_for 120 "$svc 기동(pid $(cat "$pf"), :$port)" is_up "http://localhost:$port"
}

stop_service() {
  local svc="$1" pf
  pf="$(pid_file "$svc")"
  if [[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null; then
    kill "$(cat "$pf")"
    while kill -0 "$(cat "$pf")" 2>/dev/null; do sleep 1; done
    log "$svc 종료"
  fi
  rm -f "$pf"
}

case "${1:-}" in
  up)
    start_infra
    for s in "${SERVICES[@]}"; do start_service "$s"; done
    ;;
  down)
    for s in "${SERVICES[@]}"; do stop_service "$s"; done
    docker rm -f "$KAFKA_CONTAINER" edml-redis >/dev/null 2>&1 || true
    (cd "$REPO_ROOT" && docker compose rm -sf mysql-order mysql-payment >/dev/null)
    log "정리 완료(MySQL 볼륨은 유지)"
    ;;
  start)   start_service "$2" ;;
  stop)    stop_service "$2" ;;
  restart) stop_service "$2"; start_service "$2" ;;
  status)
    for s in "${SERVICES[@]}"; do
      if is_up "http://localhost:$(port_of "$s")"; then echo "$s UP"; else echo "$s DOWN"; fi
    done
    docker ps --format '{{.Names}} {{.Status}}' | grep -E "mysql-|edml-" || true
    ;;
  *)
    sed -n '2,8p' "$0"; exit 1 ;;
esac
