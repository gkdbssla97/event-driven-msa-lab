#!/usr/bin/env bash
# S2 독성(poison) 메시지
#
# 주입: order-service DB에 주문 1건(orders PENDING + saga_state STARTED)과, 역직렬화가 불가능한
#       order-created payload(quantity가 문자열)를 가진 Outbox 행을 직접 넣는다.
#       "프로듀서 쪽 직렬화 버그로 깨진 이벤트가 Outbox에 적재됐다"는 상황이다.
#       발행은 실제 SkipLockedOutboxPublisher가 한다.
# 기대: inventory-service OrderCreatedEventListener가 역직렬화에 실패 → DefaultErrorHandler
#       (FixedBackOff 1s x 3회 재시도) 소진 → order-created.DLQ로 격리 →
#       order-service DlqRecoveryListener가 orderId를 뽑아 FAILED_POISON으로 종결 + 보상 이벤트 적재.
# 부수 증상(현재 코드의 잠재 결함): 예약이 없는 주문의 보상에서 RedisInventoryStore.compensate()가
#       IndexOutOfBoundsException을 던져 payment-failed 레코드도 payment-failed.DLQ로 격리된다.

source "$(dirname "$0")/lib.sh"

require_up order inventory

order_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
outbox_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
event_id="$(uuidgen | tr '[:upper:]' '[:lower:]')"
# quantity 를 문자열로 넣어 OrderCreatedEvent(int quantity) 역직렬화를 깨뜨린다. orderId 는 살려 둔다.
payload="{\"eventId\":\"$event_id\",\"eventType\":\"ORDER_CREATED\",\"orderId\":\"$order_id\",\"userId\":\"u-incident-s2\",\"productId\":\"P-S2\",\"quantity\":\"two\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"

recovered_before="$(metric_value "$ORDER_URL" saga_dlq_recovered_total)"
swept_before="$(metric_value "$ORDER_URL" saga_swept_total)"

section "주입: 깨진 order-created 이벤트를 Outbox에 적재"
mark_injected
# 앱은 Instant 를 UTC 로 저장한다(serverTimezone=UTC). 스위퍼 판정이 어긋나지 않게 UTC_TIMESTAMP 를 쓴다.
sql_order "START TRANSACTION;
INSERT INTO orders (id, user_id, status, created_at) VALUES ('$order_id', 'u-incident-s2', 'PENDING', UTC_TIMESTAMP(6));
INSERT INTO saga_state (saga_id, status, created_at, updated_at) VALUES ('$order_id', 'STARTED', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));
INSERT INTO outbox_events (id, topic, aggregate_id, event_type, payload, status, created_at)
  VALUES ('$outbox_id', 'order-created', '$order_id', 'ORDER_CREATED', '$payload', 'PENDING', UTC_TIMESTAMP(6));
COMMIT;"
log "주입 완료: orderId=$order_id outboxId=$outbox_id"
record_run S2 "$order_id"

section "관찰"
check "Outbox 행이 PUBLISHED(발행 자체는 성공)" wait_for 15 "outbox PUBLISHED" \
  bash -c "[[ \"\$(docker exec mysql-order mysql -uorder -porderpass orderdb -N -B -e \"SELECT status FROM outbox_events WHERE id='$outbox_id'\" 2>/dev/null)\" == PUBLISHED ]]"
check "사가가 FAILED_POISON으로 종결" wait_for 40 "saga FAILED_POISON" saga_is "$order_id" FAILED_POISON
sleep 8  # 보상 이벤트 발행 → inventory 재시도 3회(1s 간격) → payment-failed.DLQ

section "증거"
echo "  saga_state:";   sql_order "SELECT saga_id,status,created_at,updated_at FROM saga_state WHERE saga_id='$order_id'" | sed 's/^/    /'
echo "  orders:";       sql_order "SELECT id,status FROM orders WHERE id='$order_id'" | sed 's/^/    /'
echo "  order outbox:"; sql_order "SELECT event_type,topic,status FROM outbox_events WHERE aggregate_id='$order_id' ORDER BY created_at" | sed 's/^/    /'
echo "  order-created.DLQ:"
dlq="$(dlq_messages order-created.DLQ "$order_id")"
printf '%s\n' "$dlq" | cut -c1-260 | sed 's/^/    /'
echo "  payment-failed.DLQ(보상 실패, 부수 증상):"
comp_dlq="$(dlq_messages payment-failed.DLQ "$order_id")"
printf '%s\n' "$comp_dlq" | cut -c1-260 | sed 's/^/    /'
echo "  logs:"
show_log inventory-service "Publishing to DLQ: topic=order-created, key=$order_id"
show_log order-service "Saga compensated from DLQ: orderId=$order_id"

recovered_after="$(metric_value "$ORDER_URL" saga_dlq_recovered_total)"
swept_after="$(metric_value "$ORDER_URL" saga_swept_total)"
echo "  metrics: saga_dlq_recovered_total ${recovered_before} -> ${recovered_after}, saga_swept_total ${swept_before} -> ${swept_after}"

check "orders.status = CANCELED" test "$(order_status "$order_id")" = CANCELED
check "order outbox에 PAYMENT_FAILED(보상) 행 존재" \
  test -n "$(sql_order "SELECT id FROM outbox_events WHERE aggregate_id='$order_id' AND event_type='PAYMENT_FAILED'")"
check "order-created.DLQ에 해당 orderId 메시지 존재" test -n "$dlq"
check "DLQ 헤더에 역직렬화 실패 메시지" grep -qF "threw exception; Failed to deserialize order-created event" <<< "$dlq"
check "saga_dlq_recovered_total 증가" test "$recovered_after" -gt "$recovered_before"
check "saga_swept_total 변화 없음(스위퍼가 아니라 DLQ 경로로 종결)" test "$swept_after" -eq "$swept_before"
check "inventory 로그: Publishing to DLQ" log_has inventory-service "Publishing to DLQ: topic=order-created, key=$order_id"
check "order 로그: Saga compensated from DLQ" \
  log_has order-service "Saga compensated from DLQ: orderId=$order_id, dlqTopic=order-created.DLQ"
check "부수 증상: payment-failed.DLQ에 보상 실패(IndexOutOfBoundsException) 격리" \
  grep -qF "exception-cause-fqcn:java.lang.IndexOutOfBoundsException" <<< "$comp_dlq"

finish
