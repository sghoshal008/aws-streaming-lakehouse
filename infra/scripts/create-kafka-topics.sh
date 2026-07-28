#!/usr/bin/env bash
set -euo pipefail

# Fallback utility only. The primary IaC path creates these topics
# through AWS::MSK::Topic in messaging.yaml.

: "${BOOTSTRAP_SERVERS:?Set BOOTSTRAP_SERVERS first}"

KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka_2.13-3.6.0}"
CONFIG="${CONFIG:-client.properties}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local replication="$3"

  echo "Creating topic: $topic"

  "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --command-config "$CONFIG" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication"
}

create_topic "iata-sales-iac-records" 6 2
create_topic "iata-sales-iac-errors" 3 2
create_topic "iata-sales-iac-control-iceberg" 1 2

echo "Kafka topic creation completed."