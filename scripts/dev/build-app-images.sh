#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT"

./gradlew \
  :order-service:bootBuildImage --imageName=event-driven-msa-lab/order-service:dev \
  :payment-service:bootBuildImage --imageName=event-driven-msa-lab/payment-service:dev \
  :websocket-service:bootBuildImage --imageName=event-driven-msa-lab/websocket-service:dev

printf 'Built local images for order-service, payment-service, and websocket-service.\n'
