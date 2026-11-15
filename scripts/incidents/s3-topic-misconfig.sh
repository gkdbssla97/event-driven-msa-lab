#!/usr/bin/env bash
# S3 토픽 이름 오설정
#
# 주입: payment-service 를 app.kafka.topics.payment-completed=payment-complete(오타)로 재기동하고
#       정상 주문을 넣는다. 환경변수 APP_KAFKA_TOPICS_PAYMENT_COMPLETED 로 덮어쓴다.
#
# 왜 payment-service 인가: order-service·inventory-service 는 자기가 발행하는 토픽을 NewTopic 빈으로
# 선언하므로 이름을 틀려도 KafkaAdmin 이 그 이름으로 토픽을 만들어 버린다(발행은 성공, 소비자만 없음).
# payment-service 는 NewTopic 선언이 없어, 브로커 auto.create.topics.enable=false 이면
# 프로듀서가 메타데이터를 받지 못하고 max.block.ms(기본 60000ms) 동안 send() 에서 막힌다.
#
# 기대: 결제는 DB에 커밋(payment_records SUCCESS, ledger PAID, outbox PENDING)되지만 발행이 매번 실패해
#       payment outbox 에 PENDING 이 쌓이고, order-service 사가는 RESERVED 에서 멈췄다가 스위퍼가 TIMED_OUT 처리한다.
#
#   s3-topic-misconfig.sh              주입 → 관찰 → 올바른 설정으로 재기동
#   s3-topic-misconfig.sh --no-restore 관찰 후 오설정 상태로 둔다

source "$(dirname "$0")/lib.sh"

RESTORE=true
[[ "${1:-}" == "--no-restore" ]] && RESTORE=false
BAD_TOPIC="${S3_BAD_TOPIC:-payment-complete}"
ORDERS="${S3_ORDERS:-2}"

require_up order inventory payment

section "사전 조건"
auto_create="$(kafka kafka-configs.sh --describe --entity-type brokers --entity-name 1 --all 2>/dev/null \
  | grep -E '^\s*auto.create.topics.enable=' | sed -E 's/^ *auto\.create\.topics\.enable=([a-z]+) .*/\1/')"
log "broker auto.create.topics.enable=$auto_create"
if [[ "$auto_create" != false ]]; then
  log "auto.create.topics.enable 이 false 가 아니면 오타 토픽이 자동 생성돼 이 시나리오의 증상이 나오지 않습니다."
  log "KAFKA_AUTO_CREATE=false 로 local-stack.sh 를 다시 띄우세요."
  exit 2
fi
if kafka kafka-topics.sh --list | grep -qx "$BAD_TOPIC"; then
  log "토픽 $BAD_TOPIC 이 이미 존재합니다. 삭제 후 다시 실행하세요: kafka-topics.sh --delete --topic $BAD_TOPIC"
  exit 2
fi

section "주입: payment-service 토픽 이름 오설정($BAD_TOPIC)으로 재기동"
APP_KAFKA_TOPICS_PAYMENT_COMPLETED="$BAD_TOPIC" "$INCIDENT_DIR/local-stack.sh" restart payment-service
mark_injected
# 카운터는 재기동으로 0부터 시작한다.
failed_before="$(metric_value "$PAYMENT_URL" outbox_publish_failed_total)"
swept_before="$(metric_value "$ORDER_URL" saga_swept_total)"

ids=()
for i in $(seq 1 "$ORDERS"); do
  ids+=("$(create_order u-incident-s3 "P-S3-$(date +%s)" 1)")
done
log "주문 생성: ${ids[*]}"
record_run S3 "${ids[@]}"

section "관찰"
first="${ids[0]}"
check "사가가 RESERVED까지 진행" wait_for 30 "saga RESERVED" saga_is "$first" RESERVED
check "payment outbox에 오타 토픽 PENDING 행 적재" wait_for 20 "payment outbox PENDING" \
  bash -c "[[ \"\$(docker exec mysql-payment mysql -upayment -ppaymentpass paymentdb -N -B -e \"SELECT COUNT(*) FROM outbox_events WHERE topic='$BAD_TOPIC' AND status='PENDING'\" 2>/dev/null)\" -ge $ORDERS ]]"
log "max.block.ms(60s) 만큼 첫 발행이 막히는 것을 기다립니다"
check "outbox_publish_failed_total 증가" wait_for 100 "publish failed" \
  bash -c "[[ \$(curl -fsS $PAYMENT_URL/actuator/prometheus | grep -E '^outbox_publish_failed_total' | awk '{printf \"%d\", \$NF}') -gt $failed_before ]]"
wait_secs=$(( (SAGA_TIMEOUT_MS + SAGA_SWEEP_INTERVAL_MS) / 1000 + 30 ))
check "사가가 결국 스위퍼에 의해 TIMED_OUT" wait_for "$wait_secs" "saga TIMED_OUT" saga_is "$first" TIMED_OUT
sleep 3

section "증거"
in_list="$(printf "'%s'," "${ids[@]}")"; in_list="${in_list%,}"
echo "  saga_state:";      sql_order "SELECT saga_id,status,updated_at FROM saga_state WHERE saga_id IN ($in_list)" | sed 's/^/    /'
echo "  payment_records:"; sql_payment "SELECT order_id,status,processed_at FROM payment_records WHERE order_id IN ($in_list)" | sed 's/^/    /'
echo "  ledger_entries:";  sql_payment "SELECT id,type,amount FROM ledger_entries WHERE order_id IN ($in_list)" | sed 's/^/    /'
echo "  payment outbox:";  sql_payment "SELECT aggregate_id,event_type,topic,status,created_at FROM outbox_events WHERE aggregate_id IN ($in_list)" | sed 's/^/    /'
echo "  order outbox:";    sql_order "SELECT aggregate_id,event_type,topic,status FROM outbox_events WHERE aggregate_id IN ($in_list) ORDER BY created_at" | sed 's/^/    /'
echo "  topics:"; kafka kafka-topics.sh --list | sed 's/^/    /'
echo "  logs:"
show_log payment-service "not present in metadata after"

pending="$(metric_value "$PAYMENT_URL" outbox_pending)"
failed_after="$(metric_value "$PAYMENT_URL" outbox_publish_failed_total)"
swept_after="$(metric_value "$ORDER_URL" saga_swept_total)"
echo "  metrics: payment outbox_pending=${pending}, outbox_publish_failed_total ${failed_before} -> ${failed_after}, saga_swept_total ${swept_before} -> ${swept_after}"

check "payment_records SUCCESS(결제는 커밋됨)" \
  test "$(sql_payment "SELECT status FROM payment_records WHERE order_id='$first'")" = SUCCESS
check "ledger_entries에 PAID 존재" test -n "$(sql_payment "SELECT id FROM ledger_entries WHERE order_id='$first' AND type='PAID'")"
check "payment outbox_pending >= 주문 수" test "$pending" -ge "$ORDERS"
check "오타 토픽이 브로커에 생성되지 않음" bash -c "! docker exec $KAFKA_CONTAINER $KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -qx $BAD_TOPIC"
check "payment 로그: Topic $BAD_TOPIC not present in metadata after 60000 ms" \
  log_has payment-service "Topic $BAD_TOPIC not present in metadata after 60000 ms"
check "saga_swept_total 증가" test "$swept_after" -gt "$swept_before"
check "payment-service health UP(프로세스는 살아 있음)" is_up "$PAYMENT_URL"

if $RESTORE; then
  section "복구: 올바른 토픽으로 재기동 + 적체 행 토픽 수정"
  # Outbox 행은 적재 시점의 토픽 이름을 topic 컬럼에 담는다. 설정만 고쳐 재기동하면 이미 쌓인 행은
  # 여전히 오타 토픽으로 재시도돼 계속 막힌다. 그래서 프로세스를 내린 상태(행 락 없음)에서 행을 고친다.
  "$INCIDENT_DIR/local-stack.sh" stop payment-service
  sql_payment "UPDATE outbox_events SET topic='payment-completed' WHERE topic='$BAD_TOPIC' AND status='PENDING'"
  "$INCIDENT_DIR/local-stack.sh" start payment-service
  wait_for 30 "payment outbox PENDING 해소" \
    bash -c "[[ \"\$(docker exec mysql-payment mysql -upayment -ppaymentpass paymentdb -N -B -e \"SELECT COUNT(*) FROM outbox_events WHERE status='PENDING'\" 2>/dev/null)\" == 0 ]]" || true
  echo "  복구 후 payment outbox:"; sql_payment "SELECT aggregate_id,topic,status FROM outbox_events WHERE aggregate_id IN ($in_list)" | sed 's/^/    /'
  # 사가는 이미 TIMED_OUT(재고 복원 완료)이라 뒤늦은 payment-completed 는 confirm() 가드에서 무시된다.
  # 원장에는 PAID 가 남아 "결제됨 + 주문 취소" 불일치가 생긴다(정답표 참고).
  echo "  복구 후 saga_state:"; sql_order "SELECT saga_id,status FROM saga_state WHERE saga_id IN ($in_list)" | sed 's/^/    /'
fi

finish
