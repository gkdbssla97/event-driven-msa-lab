#!/usr/bin/env bash

set -euo pipefail

KAFKA_NAMESPACE="kafka"

if ! command -v helm >/dev/null 2>&1; then
  printf 'Missing required command: helm\n' >&2
  exit 1
fi

if ! command -v kubectl >/dev/null 2>&1; then
  printf 'Missing required command: kubectl\n' >&2
  exit 1
fi

helm uninstall kafka --namespace "$KAFKA_NAMESPACE" || true

printf 'Kafka release removed from namespace %s.\n' "$KAFKA_NAMESPACE"
printf 'PersistentVolumeClaims are kept by default. Delete them manually if you need a clean reset.\n'
