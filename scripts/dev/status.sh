#!/usr/bin/env bash

set -euo pipefail

KAFKA_NAMESPACE="kafka"

if ! command -v kubectl >/dev/null 2>&1; then
  printf 'Missing required command: kubectl\n' >&2
  exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
  printf 'No reachable Kubernetes cluster is configured for kubectl.\n' >&2
  exit 1
fi

printf 'Current context: '
kubectl config current-context

printf '\n[kafka namespace]\n'
kubectl get namespace "$KAFKA_NAMESPACE"

printf '\n[pods]\n'
kubectl get pods -n "$KAFKA_NAMESPACE"

printf '\n[services]\n'
kubectl get services -n "$KAFKA_NAMESPACE"

printf '\n[persistent volume claims]\n'
kubectl get pvc -n "$KAFKA_NAMESPACE"
