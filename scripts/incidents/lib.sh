#!/usr/bin/env bash
# 장애 시나리오 스크립트 공용 함수. 각 시나리오 스크립트가 source 한다.
#
# 대상 환경: scripts/incidents/local-stack.sh 로 띄운 로컬 스택
#   - Kafka(도커, edml-kafka), MySQL(docker-compose의 mysql-order / mysql-payment), Redis(도커, edml-redis)
#   - order/payment/inventory-service 는 호스트에서 java -jar 프로세스로 실행
# 로그는 $LOG_DIR/<service>.log 에 쌓인다(2단계 MCP 로그 도구가 읽을 위치).

set -euo pipefail

INCIDENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$INCIDENT_DIR/../.." && pwd)"
RUN_DIR="${INCIDENT_RUN_DIR:-$REPO_ROOT/build/incidents}"
LOG_DIR="$RUN_DIR/logs"
RUNS_DIR="$RUN_DIR/runs"
mkdir -p "$LOG_DIR" "$RUNS_DIR"

ORDER_URL="${ORDER_URL:-http://localhost:8081}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:8082}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8084}"

KAFKA_CONTAINER="${KAFKA_CONTAINER:-edml-kafka}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

# local-stack.sh 가 order-service 에 넘기는 값과 같아야 대기 시간이 맞는다.
SAGA_TIMEOUT_MS="${SAGA_TIMEOUT_MS:-60000}"
SAGA_SWEEP_INTERVAL_MS="${SAGA_SWEEP_INTERVAL_MS:-10000}"

FAILED_CHECKS=0

log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
section() { printf '\n== %s ==\n' "$*"; }

check() {
  # check "설명" <조건 명령...>
  local desc="$1"; shift
  if "$@"; then
    printf '  PASS  %s\n' "$desc"
  else
    printf '  FAIL  %s\n' "$desc"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
  fi
}

finish() {
  section "결과"
  if [[ "$FAILED_CHECKS" -eq 0 ]]; then
    log "기대 증상이 모두 관찰됐습니다."
  else
    log "기대와 다른 항목 ${FAILED_CHECKS}개"
    exit 1
  fi
}

# --- 저장소 조회 ---------------------------------------------------------------

sql_order()   { docker exec mysql-order   mysql -uorder   -porderpass   orderdb   -N -B -e "$1" 2>/dev/null; }
sql_payment() { docker exec mysql-payment mysql -upayment -ppaymentpass paymentdb -N -B -e "$1" 2>/dev/null; }

saga_status() { sql_order "SELECT status FROM saga_state WHERE saga_id='$1'"; }
order_status() { sql_order "SELECT status FROM orders WHERE id='$1'"; }

kafka() {
  # kafka <tool.sh> [args...]  예: kafka kafka-topics.sh --list
  local tool="$1"; shift
  docker exec "$KAFKA_CONTAINER" "$KAFKA_BIN/$tool" --bootstrap-server localhost:9092 "$@"
}

# DLQ 토픽에서 특정 key(=orderId)의 레코드를 찾아 헤더 요약을 출력한다.
# 헤더의 스택트레이스에 줄바꿈이 섞여 레코드 하나가 여러 줄로 찍히므로, "kafka_dlt-"로 시작하는 줄을
# 레코드 시작으로 보고 묶는다. 첫 줄(예외 클래스·메시지)과 마지막 줄(원본 토픽·key·value)만 출력한다.
# original-partition/offset 같은 헤더 값은 바이너리라 NUL 이 섞인다. macOS awk 가 거기서 문자열을 끊으므로 출력 가능한 ASCII만 남긴다.
dlq_messages() {
  local topic="$1" key="$2"
  { kafka kafka-console-consumer.sh --topic "$topic" --from-beginning --timeout-ms 8000 \
      --property print.key=true --property print.headers=true 2>/dev/null || true; } \
    | LC_ALL=C tr -cd '\11\12\40-\176' \
    | LC_ALL=C awk -v key="$key" '
        function flush() { if (rec != "" && index(rec, key) > 0) { print first; print last } }
        /^kafka_dlt-/ { flush(); rec = ""; first = $0 }
        { rec = rec $0 "\n"; last = $0 }
        END { flush() }'
}

# --- 지표 ----------------------------------------------------------------------

# metric_value <base-url> <라인 정규식>  → 매칭 라인 값의 합(없으면 0)
metric_value() {
  local url="$1" pattern="$2"
  { curl -fsS "$url/actuator/prometheus" 2>/dev/null || true; } \
    | { grep -E "^${pattern}" || true; } | awk '{s += $NF} END {printf "%d\n", s}'
}

# --- HTTP ----------------------------------------------------------------------

is_up() { curl -fsS -o /dev/null "$1/actuator/health" 2>/dev/null; }

require_up() {
  local name url
  for name in "$@"; do
    case "$name" in
      order)     url="$ORDER_URL" ;;
      payment)   url="$PAYMENT_URL" ;;
      inventory) url="$INVENTORY_URL" ;;
    esac
    if ! is_up "$url"; then
      log "${name}-service 가 응답하지 않습니다($url). scripts/incidents/local-stack.sh up 으로 먼저 띄우세요."
      exit 2
    fi
  done
}

# create_order <userId> <productId> <quantity>  → orderId 출력
create_order() {
  curl -fsS -X POST "$ORDER_URL/orders" -H 'Content-Type: application/json' \
    -d "{\"userId\":\"$1\",\"productId\":\"$2\",\"quantity\":$3}" \
    | sed -E 's/.*"orderId":"([^"]+)".*/\1/'
}

# --- 대기 ----------------------------------------------------------------------

# wait_for <timeout초> <설명> <조건 명령...>
wait_for() {
  local timeout="$1" desc="$2"; shift 2
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    if "$@"; then
      log "$desc ($((SECONDS - start))s)"
      return 0
    fi
    sleep 2
  done
  log "시간 초과: $desc (${timeout}s)"
  return 1
}

saga_is() { [[ "$(saga_status "$1")" == "$2" ]]; }

# --- 로그 ----------------------------------------------------------------------

log_has() {
  # log_has <service> <고정 문자열>
  grep -qF "$2" "$LOG_DIR/$1.log" 2>/dev/null
}

show_log() {
  # show_log <service> <고정 문자열>  → 매칭 라인 최대 5개 출력
  grep -F "$2" "$LOG_DIR/$1.log" 2>/dev/null | tail -n 5 | cut -c1-300 | sed 's/^/    /' || true
}

# --- 실행 기록 -----------------------------------------------------------------

# record_run <scenario> <orderId...>  2단계 채점 때 "이 실행의 정답"을 찾을 수 있게 남긴다.
record_run() {
  local scenario="$1"; shift
  local file="$RUNS_DIR/${scenario}-$(date -u +%Y%m%dT%H%M%SZ).json"
  local ids
  ids="$(printf '"%s",' "$@")"
  printf '{"scenario":"%s","injectedAt":"%s","orderIds":[%s]}\n' \
    "$scenario" "$INJECTED_AT" "${ids%,}" > "$file"
  log "실행 기록: $file"
}

mark_injected() { INJECTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; log "주입 시각(UTC): $INJECTED_AT"; }
