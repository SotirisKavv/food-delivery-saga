#!/usr/bin/env bash
set -euo pipefail

: "${KAFKA_BROKER:?KAFKA_BROKER is required}"
MAX_WAIT="${MAX_WAIT:-30}"
CONF="/topics.conf"

if [ ! -f "$CONF" ]; then
  echo "$(date +'%F %T') | topics.conf not found at $CONF"
  exit 1
fi

echo "$(date +'%F %T') | Waiting for Kafka broker $KAFKA_BROKER up to ${MAX_WAIT}s..."
SECONDS=0
until kafka-topics --bootstrap-server "$KAFKA_BROKER" --list >/dev/null 2>&1; do
  if [ "$SECONDS" -ge "$MAX_WAIT" ]; then
    echo "$(date +'%F %T') | Timeout waiting for Kafka broker"
    exit 1
  fi
  sleep 2
done

echo "$(date +'%F %T') | Kafka is up. Creating topics from $CONF..."
while IFS=: read -r topic partitions rf || [ -n "${topic:-}" ]; do
  [ -z "${topic:-}" ] && continue
  case "$topic" in \#*) continue;; esac
  # Normalize CRLF and trim whitespace (avoid xargs; use sed)
  topic="$(printf '%s' "$topic" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  partitions="$(printf '%s' "${partitions:-}" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  rf="$(printf '%s' "${rf:-}" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  # Defaults and numeric guards using bash regex
  [[ "$partitions" =~ ^[0-9]+$ ]] || partitions=1
  [[ "$rf" =~ ^[0-9]+$ ]] || rf=1
  echo "$(date +'%F %T') | Ensuring topic $topic (partitions=$partitions, rf=$rf)"
  kafka-topics --bootstrap-server "$KAFKA_BROKER" --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor "$rf"
done < "$CONF"

echo "$(date +'%F %T') | All topics ensured."
