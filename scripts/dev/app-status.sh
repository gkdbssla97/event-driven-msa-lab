#!/usr/bin/env bash

set -euo pipefail

if ! command -v kubectl >/dev/null 2>&1; then
  printf 'Missing required command: kubectl\n' >&2
  exit 1
fi

printf '[deployments]\n'
kubectl get deployments -n kafka order-service payment-service websocket-service

printf '\n[pods]\n'
kubectl get pods -n kafka -l 'app in (order-service,payment-service,websocket-service)'

printf '\n[services]\n'
kubectl get services -n kafka order-service payment-service websocket-service
