#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
KAFKA_NAMESPACE="kafka"
VALUES_FILE="$REPO_ROOT/infra/kafka/values-dev.yaml"

require_command() {
  local command_name="$1"

  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$command_name" >&2
    exit 1
  fi
}

require_command kubectl
require_command helm

if ! kubectl cluster-info >/dev/null 2>&1; then
  printf 'kubectl is installed, but no reachable cluster is configured.\n' >&2
  printf 'Prepare a local k3s context first, then rerun this script.\n' >&2
  exit 1
fi

if [[ ! -f "$VALUES_FILE" ]]; then
  printf 'Kafka values file not found: %s\n' "$VALUES_FILE" >&2
  exit 1
fi

kubectl get namespace "$KAFKA_NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$KAFKA_NAMESPACE"

helm repo add bitnami https://charts.bitnami.com/bitnami >/dev/null 2>&1 || true
helm repo update >/dev/null

helm upgrade --install kafka bitnami/kafka \
  --namespace "$KAFKA_NAMESPACE" \
  --create-namespace \
  --values "$VALUES_FILE"

printf '\nKafka install command finished.\n'
printf 'Next: run scripts/dev/status.sh to inspect pods, services, and PVCs.\n'
