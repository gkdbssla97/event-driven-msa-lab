#!/usr/bin/env bash
# S4 재고 부족(대조군, 장애 아님)
#
# 주입: 새 상품의 재고를 1로 초기화하고 수량 5로 주문한다.
# 기대: inventory-service 가 RESERVE_SCRIPT 에서 -1 을 받아 inventory-failed 발행 →
#       order-service 가 주문 CANCELED, 사가 FAILED_INSUFFICIENT_STOCK. DLQ·스위퍼·보상은 관여하지 않는다.
# 진단 도구는 이것을 "장애 아님(정상적인 비즈니스 실패)"으로 판정해야 한다.

source "$(dirname "$0")/lib.sh"

require_up order inventory payment

PRODUCT="P-S4-$(date +%s)"

reject_before="$(metric_value "$INVENTORY_URL" 'inventory_reserve_total\{.*result="insufficient_stock"')"
swept_before="$(metric_value "$ORDER_URL" saga_swept_total)"
recovered_before="$(metric_value "$ORDER_URL" saga_dlq_recovered_total)"

section "주입: 재고 1개 상품에 수량 5 주문"
curl -fsS -X POST "$INVENTORY_URL/inventory/$PRODUCT/init?stock=1" >/dev/null
mark_injected
order_id="$(create_order u-incident-s4 "$PRODUCT" 5)"
log "주문 생성: orderId=$order_id productId=$PRODUCT quantity=5 (stock=1)"
record_run S4 "$order_id"

section "관찰"
check "사가가 FAILED_INSUFFICIENT_STOCK으로 종결" wait_for 30 "saga FAILED_INSUFFICIENT_STOCK" \
  saga_is "$order_id" FAILED_INSUFFICIENT_STOCK
sleep 3

section "증거"
echo "  saga_state:";   sql_order "SELECT saga_id,status,created_at,updated_at FROM saga_state WHERE saga_id='$order_id'" | sed 's/^/    /'
echo "  orders:";       sql_order "SELECT id,status FROM orders WHERE id='$order_id'" | sed 's/^/    /'
echo "  order outbox:"; sql_order "SELECT event_type,topic,status FROM outbox_events WHERE aggregate_id='$order_id'" | sed 's/^/    /'
echo "  stock:"; curl -fsS "$INVENTORY_URL/inventory/$PRODUCT" | sed 's/^/    /'; echo
echo "  logs:"
show_log inventory-service "Insufficient stock: productId=$PRODUCT"
show_log order-service "Inventory failed: orderId=$order_id"

reject_after="$(metric_value "$INVENTORY_URL" 'inventory_reserve_total\{.*result="insufficient_stock"')"
swept_after="$(metric_value "$ORDER_URL" saga_swept_total)"
recovered_after="$(metric_value "$ORDER_URL" saga_dlq_recovered_total)"
echo "  metrics: inventory_reserve_total{insufficient_stock} ${reject_before} -> ${reject_after}, saga_swept_total ${swept_before} -> ${swept_after}, saga_dlq_recovered_total ${recovered_before} -> ${recovered_after}"

check "orders.status = CANCELED" test "$(order_status "$order_id")" = CANCELED
check "order outbox에는 ORDER_CREATED 1건만(보상 이벤트 없음)" \
  test "$(sql_order "SELECT GROUP_CONCAT(event_type) FROM outbox_events WHERE aggregate_id='$order_id'")" = ORDER_CREATED
check "payment_records에 해당 주문 없음" test -z "$(sql_payment "SELECT order_id FROM payment_records WHERE order_id='$order_id'")"
check "재고가 1 그대로(차감되지 않음)" grep -q '"stock":1' <<< "$(curl -fsS "$INVENTORY_URL/inventory/$PRODUCT")"
check "inventory_reserve_total{result=insufficient_stock} 증가" test "$reject_after" -gt "$reject_before"
check "saga_swept_total 변화 없음" test "$swept_after" -eq "$swept_before"
check "saga_dlq_recovered_total 변화 없음" test "$recovered_after" -eq "$recovered_before"
check "order-created.DLQ에 해당 orderId 없음" test -z "$(dlq_messages order-created.DLQ "$order_id")"
check "inventory 로그: Insufficient stock" log_has inventory-service "Insufficient stock: productId=$PRODUCT, requested=5"
check "order 로그: Inventory failed ... reason=Insufficient stock" \
  log_has order-service "Inventory failed: orderId=$order_id, reason=Insufficient stock"

finish
