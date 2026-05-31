#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
KUSTOMIZE_DIR="$REPO_ROOT/infra/k3s/apps"
CLUSTER_NAME="event-driven-msa-lab"

require_command() {
  local command_name="$1"

  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$command_name" >&2
    exit 1
  fi
}

require_command kubectl
require_command k3d

if ! kubectl cluster-info >/dev/null 2>&1; then
  printf 'kubectl is installed, but no reachable cluster is configured.\n' >&2
  exit 1
fi

if [[ ! -d "$KUSTOMIZE_DIR" ]]; then
  printf 'Kustomize directory not found: %s\n' "$KUSTOMIZE_DIR" >&2
  exit 1
fi

k3d image import \
  event-driven-msa-lab/order-service:dev \
  event-driven-msa-lab/payment-service:dev \
  event-driven-msa-lab/websocket-service:dev \
  -c "$CLUSTER_NAME"

kubectl apply -k "$KUSTOMIZE_DIR"

kubectl rollout status deployment/order-service -n kafka --timeout=300s
kubectl rollout status deployment/payment-service -n kafka --timeout=300s
kubectl rollout status deployment/websocket-service -n kafka --timeout=300s

printf 'Application deployments rolled out in namespace kafka.\n'
