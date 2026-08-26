#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

VALID_EVENT_COUNT="${SMOKE_VALID_EVENT_COUNT:-20}"
INVALID_EVENT_COUNT="${SMOKE_INVALID_EVENT_COUNT:-20}"
WAIT_SECONDS="${SMOKE_WAIT_SECONDS:-90}"
PYTHON_BIN="${PYTHON_BIN:-python}"
LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/payment-spark-smoke.XXXXXX")"
STREAM_PID=""
SMOKE_SUCCEEDED=false

cleanup() {
  if [[ -n "$STREAM_PID" ]] && kill -0 "$STREAM_PID" 2>/dev/null; then
    kill "$STREAM_PID" 2>/dev/null || true
    wait "$STREAM_PID" 2>/dev/null || true
  fi

  if [[ "$SMOKE_SUCCEEDED" == true ]]; then
    rm -f "$LOG_FILE"
  else
    echo "Spark log retained at: $LOG_FILE" >&2
  fi
}
trap cleanup EXIT INT TERM

for command in docker spark-submit "$PYTHON_BIN"; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "Required command not found: $command" >&2
    exit 1
  fi
done

query_count() {
  local table="$1"
  docker compose exec -T postgres psql -U payments -d payments -Atc "SELECT COUNT(*) FROM ${table};" \
    | tr -d '[:space:]'
}

echo "Starting Kafka and PostgreSQL..."
docker compose up -d --wait
docker compose exec -T postgres psql -U payments -d payments < sql/init.sql >/dev/null
./scripts/create_topic.sh

valid_before="$(query_count payments)"
rejected_before="$(query_count rejected_payments)"
quality_before="$(
  docker compose exec -T postgres psql -U payments -d payments -Atc \
    "SELECT COALESCE(SUM(processed_count), 0) FROM streaming_batch_quality WHERE stream_name = 'payment-quality';" \
    | tr -d '[:space:]'
)"

echo "Starting Spark Structured Streaming..."
PYTHONUNBUFFERED=1 spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.13 \
  src/streaming/payment_stream.py >"$LOG_FILE" 2>&1 &
STREAM_PID=$!

stream_ready=false
for ((second = 0; second < WAIT_SECONDS; second++)); do
  if ! kill -0 "$STREAM_PID" 2>/dev/null; then
    echo "Spark exited before the query became ready." >&2
    tail -n 80 "$LOG_FILE" >&2
    exit 1
  fi
  if grep -q "streaming from payments.raw" "$LOG_FILE"; then
    stream_ready=true
    break
  fi
  sleep 1
done

if [[ "$stream_ready" != true ]]; then
  echo "Spark did not become ready within ${WAIT_SECONDS} seconds." >&2
  tail -n 80 "$LOG_FILE" >&2
  exit 1
fi

echo "Publishing ${VALID_EVENT_COUNT} guaranteed-valid events..."
"$PYTHON_BIN" -m src.producer.generate_payments \
  --count "$VALID_EVENT_COUNT" \
  --rate 50 \
  --invalid-rate 0 \
  --duplicate-rate 0

echo "Publishing ${INVALID_EVENT_COUNT} guaranteed-invalid events..."
"$PYTHON_BIN" -m src.producer.generate_payments \
  --count "$INVALID_EVENT_COUNT" \
  --rate 50 \
  --invalid-rate 1 \
  --duplicate-rate 0

expected_valid=$((valid_before + VALID_EVENT_COUNT))
expected_rejected=$((rejected_before + INVALID_EVENT_COUNT))
valid_after="$valid_before"
rejected_after="$rejected_before"
quality_after="$quality_before"

for ((second = 0; second < WAIT_SECONDS; second++)); do
  if ! kill -0 "$STREAM_PID" 2>/dev/null; then
    echo "Spark exited while waiting for PostgreSQL writes." >&2
    tail -n 80 "$LOG_FILE" >&2
    exit 1
  fi

  valid_after="$(query_count payments)"
  rejected_after="$(query_count rejected_payments)"
  quality_after="$(
    docker compose exec -T postgres psql -U payments -d payments -Atc \
      "SELECT COALESCE(SUM(processed_count), 0) FROM streaming_batch_quality WHERE stream_name = 'payment-quality';" \
      | tr -d '[:space:]'
  )"
  if ((valid_after >= expected_valid \
      && rejected_after >= expected_rejected \
      && quality_after >= quality_before + VALID_EVENT_COUNT + INVALID_EVENT_COUNT)); then
    SMOKE_SUCCEEDED=true
    echo "Smoke test passed: valid +$((valid_after - valid_before)), rejected +$((rejected_after - rejected_before)), quality events +$((quality_after - quality_before))."
    exit 0
  fi
  sleep 1
done

echo "Timed out waiting for expected PostgreSQL rows." >&2
echo "Expected at least: valid=${expected_valid}, rejected=${expected_rejected}" >&2
echo "Observed: valid=${valid_after}, rejected=${rejected_after}" >&2
echo "Quality events: expected at least $((quality_before + VALID_EVENT_COUNT + INVALID_EVENT_COUNT)), observed=${quality_after}" >&2
tail -n 80 "$LOG_FILE" >&2
exit 1
