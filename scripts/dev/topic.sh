#!/usr/bin/env bash

set -euo pipefail

KAFKA_NAMESPACE="kafka"

if ! command -v kubectl >/dev/null 2>&1; then
  printf 'Missing required command: kubectl\n' >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  printf 'Usage:\n' >&2
  printf '  scripts/dev/topic.sh list\n' >&2
  printf '  scripts/dev/topic.sh create <topic-name>\n' >&2
  exit 1
fi

POD_NAME="$(kubectl get pods -n "$KAFKA_NAMESPACE" -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}')"

if [[ -z "$POD_NAME" ]]; then
  printf 'No Kafka pod found in namespace %s.\n' "$KAFKA_NAMESPACE" >&2
  exit 1
fi

case "$1" in
  list)
    kubectl exec -n "$KAFKA_NAMESPACE" "$POD_NAME" -- kafka-topics.sh --bootstrap-server kafka:9092 --list
    ;;
  create)
    if [[ $# -ne 2 ]]; then
      printf 'Usage: scripts/dev/topic.sh create <topic-name>\n' >&2
      exit 1
    fi

    kubectl exec -n "$KAFKA_NAMESPACE" "$POD_NAME" -- \
      kafka-topics.sh \
      --bootstrap-server kafka:9092 \
      --create \
      --if-not-exists \
      --topic "$2" \
      --partitions 1 \
      --replication-factor 1
    ;;
  *)
    printf 'Unsupported command: %s\n' "$1" >&2
    exit 1
    ;;
esac
