#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
exec go run ./cmd/etcd-lite-local "$@"
