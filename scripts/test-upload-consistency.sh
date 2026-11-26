#!/usr/bin/env bash
# Runs the upload finalization consistency suite against a REAL PostgreSQL.
# Object storage is an in-memory recording fake (and an httptest S3 mock for
# the production adapter), so no cloud credentials are needed.
#
# Usage:
#   scripts/test-upload-consistency.sh           # start docker DB if needed, run tests
#   TEST_DATABASE_DSN=... scripts/test-upload-consistency.sh   # use existing Postgres
set -euo pipefail

cd "$(dirname "$0")/.."

DSN="${TEST_DATABASE_DSN:-postgres://dekho:dekho@127.0.0.1:5433/dekho_test?sslmode=disable}"
export TEST_DATABASE_DSN="$DSN"

if [[ -z "${SKIP_DB_BOOTSTRAP:-}" ]]; then
  if ! go run ./scripts/pgready "$DSN" 2>/dev/null; then
    echo ">> Starting test PostgreSQL via docker compose..."
    docker compose -f docker-compose.test.yml up -d --wait
    echo ">> Waiting for PostgreSQL to accept connections..."
    for _ in $(seq 1 60); do
      if go run ./scripts/pgready "$DSN" 2>/dev/null; then break; fi
      sleep 1
    done
  fi
fi

echo ">> go vet"
go vet ./...

echo ">> go test -race (upload consistency + full suite)"
go test -race -count=1 ./...
