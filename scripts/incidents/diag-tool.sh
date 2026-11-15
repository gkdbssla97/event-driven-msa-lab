#!/usr/bin/env bash
# 진단 프로토타입용 읽기 전용 조회 도구. claude -p 에는 이 스크립트 하나만 허용한다.
# 2단계 Kotlin MCP 서버(사가·Outbox·DLQ·지표·로그 조회)가 나오기 전의 임시 대역이다.
#
#   diag-tool.sh help
#   diag-tool.sh health
#   diag-tool.sh db <order|payment> "<SELECT|SHOW|DESCRIBE ...>"
#   diag-tool.sh metrics <order|payment|inventory>
#   diag-tool.sh logs <order-service|payment-service|inventory-service> "<고정 문자열>" [줄 수, 최대 50]
#   diag-tool.sh logs-tail <order-service|payment-service|inventory-service> [줄 수, 최대 50]
#   diag-tool.sh topics
#   diag-tool.sh dlq <토픽.DLQ> <key>
#
# 읽기 전용 보장(프로토타입 수준):
#   - SQL은 SELECT/SHOW/DESCRIBE/EXPLAIN 한 문장만, 허용 문자 외(; \ ` 줄바꿈 등)는 거부,
#     INTO OUTFILE·FOR UPDATE·LOCK·SLEEP 같은 부작용 구문 거부, START TRANSACTION READ ONLY 안에서 실행.
#   - 앱 계정으로 접속하므로 DB 권한 자체는 쓰기가 가능하다. MCP 서버에서는 SELECT 전용 계정을 써야 한다.

source "$(dirname "$0")/lib.sh"

MAX_OUT=200

die() { printf 'error: %s\n' "$*" >&2; exit 64; }
cap() { head -n "$MAX_OUT"; }

svc_url() {
  case "$1" in
    order) echo "$ORDER_URL" ;; payment) echo "$PAYMENT_URL" ;; inventory) echo "$INVENTORY_URL" ;;
    *) die "service must be order|payment|inventory" ;;
  esac
}

check_log_service() {
  [[ "$1" =~ ^(order|payment|inventory)-service$ ]] || die "service must be order-service|payment-service|inventory-service"
}

check_lines() {
  local n="${1:-20}"
  [[ "$n" =~ ^[0-9]+$ ]] || die "lines must be a number"
  (( n > 50 )) && n=50
  echo "$n"
}

check_sql() {
  local q="$1" upper
  local chars_re="^[A-Za-z0-9_ ,.*'=<>()%:+/!-]+\$"
  local verb_re='^ *(SELECT|SHOW|DESCRIBE|DESC|EXPLAIN) '
  [[ "$q" =~ $chars_re ]] || die "SQL contains disallowed characters (single statement, no ; \\ \` \" or newlines)"
  upper="$(printf '%s' "$q" | tr '[:lower:]' '[:upper:]')"
  [[ "$upper" =~ $verb_re ]] || die "only SELECT/SHOW/DESCRIBE/EXPLAIN are allowed"
  local bad
  for bad in 'INTO OUTFILE' 'INTO DUMPFILE' 'FOR UPDATE' 'FOR SHARE' 'LOCK IN' 'SLEEP(' 'BENCHMARK(' 'GET_LOCK('; do
    [[ "$upper" == *"$bad"* ]] && die "disallowed clause: $bad"
  done
  return 0
}

cmd="${1:-help}"; shift || true
case "$cmd" in
  help)
    sed -n '4,12p' "$0" | sed 's/^#   //'
    ;;
  health)
    for s in order payment inventory; do
      url="$(svc_url "$s")"
      if body="$(curl -fsS --max-time 3 "$url/actuator/health" 2>&1)"; then
        echo "$s-service: $body"
      else
        echo "$s-service: UNREACHABLE ($body)"
      fi
    done
    ;;
  db)
    [[ $# -eq 2 ]] || die "usage: db <order|payment> \"<SQL>\""
    check_sql "$2"
    case "$1" in
      order)   docker exec mysql-order   mysql -uorder   -porderpass   orderdb   -B -e "START TRANSACTION READ ONLY; $2; ROLLBACK;" 2>&1 | grep -v 'Using a password' | cap ;;
      payment) docker exec mysql-payment mysql -upayment -ppaymentpass paymentdb -B -e "START TRANSACTION READ ONLY; $2; ROLLBACK;" 2>&1 | grep -v 'Using a password' | cap ;;
      *) die "db must be order|payment" ;;
    esac
    ;;
  metrics)
    [[ $# -eq 1 ]] || die "usage: metrics <order|payment|inventory>"
    url="$(svc_url "$1")"
    # JVM·HTTP 기본 지표는 빼고 도메인 지표만 보여 준다.
    { curl -fsS --max-time 3 "$url/actuator/prometheus" 2>&1 || echo "UNREACHABLE: $url"; } \
      | grep -E '^(saga_|outbox_|inventory_|payment_|kafka_consumer_fetch_manager_records_lag_max|UNREACHABLE|curl)' | cap
    ;;
  logs)
    [[ $# -ge 2 ]] || die "usage: logs <service> \"<fixed string>\" [lines]"
    check_log_service "$1"
    [[ "$2" =~ ^[^[:cntrl:]]+$ ]] || die "pattern must be a single line"
    n="$(check_lines "${3:-20}")"
    { grep -F -- "$2" "$LOG_DIR/$1.log" 2>/dev/null || true; } | tail -n "$n" | cut -c1-400
    ;;
  logs-tail)
    [[ $# -ge 1 ]] || die "usage: logs-tail <service> [lines]"
    check_log_service "$1"
    n="$(check_lines "${2:-20}")"
    tail -n "$n" "$LOG_DIR/$1.log" 2>/dev/null | cut -c1-400
    ;;
  topics)
    kafka kafka-topics.sh --list 2>&1 | cap
    ;;
  dlq)
    [[ $# -eq 2 ]] || die "usage: dlq <topic.DLQ> <key>"
    [[ "$1" =~ ^[a-z-]+\.DLQ$ ]] || die "topic must look like <name>.DLQ"
    [[ "$2" =~ ^[A-Za-z0-9-]+$ ]] || die "key must be alphanumeric/dash"
    out="$(dlq_messages "$1" "$2")"
    [[ -n "$out" ]] && printf '%s\n' "$out" | cut -c1-600 || echo "(no record with key $2 in $1)"
    ;;
  *)
    die "unknown command: $cmd (try: help)"
    ;;
esac
