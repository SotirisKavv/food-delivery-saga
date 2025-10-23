#!/bin/sh
set -eu

# Required env vars
if [ "${KAFKA_BROKER:-}" = "" ]; then
  echo "$(date +'%F %T') | KAFKA_BROKER is required"
  exit 1
fi

MAX_WAIT="${MAX_WAIT:-30}"
CONF="/topics.conf"

if [ ! -f "$CONF" ]; then
  echo "$(date +'%F %T') | topics.conf not found at $CONF"
  exit 1
fi

echo "$(date +'%F %T') | Waiting for Kafka broker $KAFKA_BROKER up to ${MAX_WAIT}s..."
elapsed=0
while ! kafka-topics --bootstrap-server "$KAFKA_BROKER" --list >/dev/null 2>&1; do
  if [ "$elapsed" -ge "$MAX_WAIT" ]; then
    echo "$(date +'%F %T') | Timeout waiting for Kafka broker"
    exit 1
  fi
  sleep 2
  elapsed=$((elapsed + 2))
done

echo "$(date +'%F %T') | Kafka is up. Creating topics from $CONF..."
# Read file line-by-line, handle CRLF, comments, and blanks
while IFS= read -r line || [ -n "$line" ]; do
  # Strip CR and trim whitespace
  line=$(printf '%s' "$line" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  [ -z "$line" ] && continue
  case "$line" in 
    \#*) continue ;;
  esac

  topic=$(printf '%s' "$line" | cut -d: -f1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  partitions=$(printf '%s' "$line" | cut -d: -f2 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  rf=$(printf '%s' "$line" | cut -d: -f3 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

  # Defaults if not numeric
  case "$partitions" in ''|*[!0-9]*) partitions=1 ;; esac
  case "$rf" in ''|*[!0-9]*) rf=1 ;; esac

  echo "$(date +'%F %T') | Ensuring topic $topic (partitions=$partitions, rf=$rf)"
  kafka-topics --bootstrap-server "$KAFKA_BROKER" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$rf"
done < "$CONF"

echo "$(date +'%F %T') | All topics ensured."
