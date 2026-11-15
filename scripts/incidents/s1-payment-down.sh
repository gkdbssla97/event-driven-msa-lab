#!/usr/bin/env bash
# S1 payment-service 중지
#
# 주입: payment-service 프로세스를 내린 뒤 정상 주문을 1건 넣는다.
# 기대: 재고는 예약돼 사가가 RESERVED에서 멈추고, SagaTimeoutSweeper가
#       app.saga.timeout-ms 경과 후 TIMED_OUT으로 종결 + payment-failed 보상 이벤트를 Outbox에 적재,
#       inventory-service가 그 이벤트로 재고를 복원한다.
#
#   s1-payment-down.sh             주입 → 관찰 → payment-service 재기동
#   s1-payment-down.sh --no-restore  관찰 후 payment-service를 내린 채로 둔다(2단계 진단 실습용)

source "$(dirname "$0")/lib.sh"

RESTORE=true
[[ "${1:-}" == "--no-restore" ]] && RESTORE=false
PRODUCT="P-S1-$(date +%s)"

require_up order inventory payment

swept_before="$(metric_value "$ORDER_URL" saga_swept_total)"
restored_before="$(metric_value "$INVENTORY_URL" 'inventory_compensate_total\{.*result="restored"')"

section "주입: payment-service 중지"
"$INCIDENT_DIR/local-stack.sh" stop payment-service
mark_injected

order_id="$(create_order u-incident-s1 "$PRODUCT" 1)"
log "주문 생성: orderId=$order_id productId=$PRODUCT"
record_run S1 "$order_id"

section "관찰"
check "사가가 RESERVED까지 진행" wait_for 30 "saga RESERVED" saga_is "$order_id" RESERVED
wait_secs=$(( (SAGA_TIMEOUT_MS + SAGA_SWEEP_INTERVAL_MS) / 1000 + 30 ))
log "스위퍼 대기(timeout ${SAGA_TIMEOUT_MS}ms + interval ${SAGA_SWEEP_INTERVAL_MS}ms, 최대 ${wait_secs}s)"
check "사가가 TIMED_OUT으로 종결" wait_for "$wait_secs" "saga TIMED_OUT" saga_is "$order_id" TIMED_OUT
sleep 5  # 보상 이벤트 발행 + inventory 복원 대기

section "증거"
echo "  saga_state:";   sql_order "SELECT saga_id,status,created_at,updated_at FROM saga_state WHERE saga_id='$order_id'" | sed 's/^/    /'
echo "  orders:";       sql_order "SELECT id,status FROM orders WHERE id='$order_id'" | sed 's/^/    /'
echo "  order outbox:"; sql_order "SELECT event_type,topic,status,created_at,published_at FROM outbox_events WHERE aggregate_id='$order_id' ORDER BY created_at" | sed 's/^/    /'
echo "  payment_records(행이 없어야 함):"; sql_payment "SELECT order_id,status FROM payment_records WHERE order_id='$order_id'" | sed 's/^/    /'
echo "  logs:"
show_log order-service "Saga timed out and compensated: sagaId=$order_id"
show_log inventory-service "Compensation complete (atomic): orderId=$order_id"

swept_after="$(metric_value "$ORDER_URL" saga_swept_total)"
restored_after="$(metric_value "$INVENTORY_URL" 'inventory_compensate_total\{.*result="restored"')"
echo "  metrics: saga_swept_total ${swept_before} -> ${swept_after}, inventory_compensate_total{restored} ${restored_before} -> ${restored_after}"

check "orders.status = CANCELED" test "$(order_status "$order_id")" = CANCELED
check "order outbox에 PAYMENT_FAILED(보상) 행이 PUBLISHED" \
  test "$(sql_order "SELECT status FROM outbox_events WHERE aggregate_id='$order_id' AND event_type='PAYMENT_FAILED'")" = PUBLISHED
check "payment_records에 해당 주문 없음(결제 미처리)" \
  test -z "$(sql_payment "SELECT order_id FROM payment_records WHERE order_id='$order_id'")"
check "saga_swept_total 증가" test "$swept_after" -gt "$swept_before"
check "inventory_compensate_total{result=restored} 증가" test "$restored_after" -gt "$restored_before"
check "payment-service health 응답 없음" bash -c "! curl -fs -o /dev/null $PAYMENT_URL/actuator/health"
check "order 로그: Saga timed out ... lastStatus=RESERVED" \
  log_has order-service "Saga timed out and compensated: sagaId=$order_id, lastStatus=RESERVED"
check "inventory 로그: Compensation complete" log_has inventory-service "Compensation complete (atomic): orderId=$order_id"

if $RESTORE; then
  section "복구: payment-service 재기동"
  "$INCIDENT_DIR/local-stack.sh" start payment-service
  # 재기동한 payment-service는 밀린 inventory-reserved를 소비해 결제를 처리한다.
  # 사가는 이미 종착이라 order-service의 confirm()은 무시되지만, 원장에는 PAID가 남는다(정답표 참고).
  sleep 8
  echo "  복구 후 payment_records:"; sql_payment "SELECT order_id,status,processed_at FROM payment_records WHERE order_id='$order_id'" | sed 's/^/    /'
  echo "  복구 후 saga_state:"; sql_order "SELECT status FROM saga_state WHERE saga_id='$order_id'" | sed 's/^/    /'
fi

finish
