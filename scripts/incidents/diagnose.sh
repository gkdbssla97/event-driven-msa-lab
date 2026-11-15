#!/usr/bin/env bash
# 진단 프로토타입: 장애 실행 기록 하나를 받아 claude -p 로 원인을 진단하고 정답표 기준으로 채점한다.
#
#   scripts/incidents/s1-payment-down.sh --no-restore      # 장애 상태를 유지한 채로(S3도 --no-restore, S2·S4는 그대로)
#   scripts/incidents/diagnose.sh build/incidents/runs/S1-<UTC>.json
#   scripts/incidents/local-stack.sh start payment-service # 진단 후 복구
#
# 인증: 구독 로그인을 쓴다(--bare 미사용). ANTHROPIC_API_KEY 가 설정돼 있으면 API 과금으로 바뀌므로 중단한다.
# 격리: 빈 임시 디렉터리에서 실행하고 도구는 Bash 하나, 그중 diag-tool.sh 만 허용한다.
#       레포 소스와 정답표(docs/incidents/answer-key.md)는 진단 모델이 읽을 수 없다.
# 환경변수: DIAG_MODEL(기본 sonnet, 예: opus), DIAG_TIMEOUT_S(기본 600)
# 결과: build/incidents/reports/<실행기록 이름>.{raw.json,report.json,grade.txt}
#       채점 요약은 build/incidents/reports/scores.jsonl 에 한 줄씩 쌓인다(적중률 집계용, 기준은 grade.py).

source "$(dirname "$0")/lib.sh"

RUN_FILE="${1:-}"
[[ -f "$RUN_FILE" ]] || { echo "usage: $0 <build/incidents/runs/*.json>" >&2; exit 64; }
[[ -z "${ANTHROPIC_API_KEY:-}" ]] || { log "ANTHROPIC_API_KEY 가 설정돼 있어 구독 대신 API 과금이 됩니다. unset 후 다시 실행하세요."; exit 2; }
command -v claude >/dev/null || { log "claude CLI 가 없습니다."; exit 2; }
if [[ -n "${CLAUDECODE:-}" ]]; then
  # Claude Code(데스크톱 앱 등) 세션 안에서 실행하면 호스트가 넣은 ANTHROPIC_BASE_URL·인증 변수를 물려받아
  # 하위 claude -p 가 401로 실패할 수 있다. 일반 터미널에서 실행하거나 env -i 로 환경을 비워 실행한다.
  log "경고: Claude Code 세션 안에서 실행 중입니다. 401이 나면 일반 터미널에서 실행하세요."
fi

REPORT_DIR="$RUN_DIR/reports"; mkdir -p "$REPORT_DIR"
name="$(basename "$RUN_FILE" .json)"
TOOL="$INCIDENT_DIR/diag-tool.sh"

# 실행 기록에서 시나리오 이름은 채점에만 쓰고 프롬프트에는 넣지 않는다(정답 누설 방지).
read -r scenario injected_at order_ids < <(python3 -c '
import json,sys
r=json.load(open(sys.argv[1]))
print(r["scenario"], r["injectedAt"], ",".join(r["orderIds"]))' "$RUN_FILE")

SCHEMA='{
  "type": "object",
  "required": ["summary", "is_incident", "candidates"],
  "additionalProperties": false,
  "properties": {
    "summary": {"type": "string"},
    "is_incident": {"type": "boolean", "description": "false면 장애가 아니라 정상적인 비즈니스 실패"},
    "candidates": {
      "type": "array", "minItems": 1, "maxItems": 3,
      "items": {
        "type": "object",
        "required": ["rank", "component", "category", "cause", "confidence", "evidence"],
        "additionalProperties": false,
        "properties": {
          "rank": {"type": "integer", "minimum": 1},
          "component": {"enum": ["order-service", "inventory-service", "payment-service", "kafka", "mysql", "redis", "none"]},
          "category": {"enum": ["service_down", "poison_message", "config_error", "broker_issue", "resource_exhaustion", "code_defect", "business_failure", "unknown"]},
          "cause": {"type": "string"},
          "confidence": {"type": "number", "minimum": 0, "maximum": 1},
          "evidence": {
            "type": "array", "minItems": 1,
            "items": {
              "type": "object", "required": ["source", "detail"], "additionalProperties": false,
              "properties": {
                "source": {"enum": ["health", "db", "metrics", "logs", "dlq", "topics"]},
                "detail": {"type": "string"}
              }
            }
          }
        }
      }
    }
  }
}'

PROMPT="당신은 아래 시스템의 온콜 엔지니어입니다. 장애 신고를 받고 원인을 조사합니다.

## 시스템
Kafka 기반 Choreography Saga로 주문을 처리합니다.
- order-service: POST /orders → orders(PENDING), saga_state(STARTED), outbox_events(ORDER_CREATED)를 한 트랜잭션에 저장. Outbox 폴러가 order-created 토픽으로 발행.
- inventory-service: order-created 소비 → Redis 재고 차감 → inventory-reserved 또는 inventory-failed 발행. payment-failed 소비 시 재고 복원(보상).
- payment-service: inventory-reserved 소비 → payment_records, ledger_entries, outbox_events(PAYMENT_COMPLETED) 저장. Outbox 폴러가 payment-completed로 발행.
- order-service는 inventory-reserved에서 saga RESERVED, payment-completed에서 COMPLETED/CONFIRMED, inventory-failed에서 FAILED_INSUFFICIENT_STOCK으로 전이.
- 사가 종착 상태: COMPLETED, FAILED_INSUFFICIENT_STOCK, COMPENSATED, TIMED_OUT(스위퍼가 멈춘 사가를 타임아웃 처리), FAILED_POISON(DLQ 복구).
- DB: order DB(orders, saga_state, outbox_events), payment DB(payment_records, ledger_entries, outbox_events). DB 시각은 UTC, 서비스 로그 시각은 +09:00.

## 조사 도구
이 명령만 쓸 수 있고 모두 읽기 전용입니다. 먼저 help를 보세요.
  $TOOL help

## 신고 내용
$injected_at(UTC) 무렵 접수된 주문이 정상 처리되지 않았다는 신고가 있습니다. 주문 ID: $order_ids

## 요구 사항
- 추측이 아니라 도구로 직접 확인한 사실을 근거로 원인 후보를 최대 3개, 가능성 높은 순으로 내세요.
- 결과로 나타난 현상(예: 타임아웃 처리, 주문 취소)과 그 원인을 구분하세요.
- 장애가 아니라 정상적인 비즈니스 실패라고 판단되면 is_incident=false로 답하세요.
- evidence.detail에는 확인한 값(상태, 행, 지표 값, 로그 문구)을 구체적으로 적으세요."

section "진단 실행: $name (주문 ${order_ids})"
workdir="$(mktemp -d)"
log "작업 디렉터리(빈 폴더): $workdir"
args=(-p "$PROMPT"
  --output-format json
  --json-schema "$SCHEMA"
  --tools Bash
  --allowedTools "Bash($TOOL *)"
  --strict-mcp-config --mcp-config '{"mcpServers":{}}'
  --no-session-persistence)
# 사용자 기본 모델이 1M 컨텍스트 버전이면 구독(Pro)에서는 사용 크레딧 없이 429가 난다. 표준 컨텍스트 모델로 고정한다.
args+=(--model "${DIAG_MODEL:-sonnet}")

# timeout 은 macOS 기본 명령이 아니라(coreutils) 있을 때만 건다.
to=(); command -v timeout >/dev/null && to=(timeout "${DIAG_TIMEOUT_S:-600}")
start=$SECONDS
( cd "$workdir" && "${to[@]}" claude "${args[@]}" < /dev/null ) > "$REPORT_DIR/$name.raw.json" \
  || { log "claude -p 실패(exit $?). $REPORT_DIR/$name.raw.json 확인"; exit 1; }
log "완료 ($((SECONDS - start))s)"
rmdir "$workdir" 2>/dev/null || true

section "리포트와 채점"
python3 "$INCIDENT_DIR/grade.py" "$REPORT_DIR/$name.raw.json" "$REPORT_DIR/$name.report.json" "$scenario" \
  "$REPORT_DIR/scores.jsonl" | tee "$REPORT_DIR/$name.grade.txt"
