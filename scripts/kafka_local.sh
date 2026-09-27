#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${KAFKA_HOME:?Set KAFKA_HOME to an extracted Apache Kafka 3.9.1 directory}"
: "${JAVA_HOME:?Set JAVA_HOME to a Java 17 installation}"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms256m -Xmx512m}"
mkdir -p data/kafka logs
case "${1:-start}" in
  start)
    if [[ ! -f data/kafka/meta.properties ]]; then
      "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c config/kafka.properties
    fi
    exec "$KAFKA_HOME/bin/kafka-server-start.sh" config/kafka.properties
    ;;
  topic)
    exec "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic order-events --partitions 3 --replication-factor 1
    ;;
  offsets)
    exec "$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic order-events
    ;;
  *) echo 'Usage: scripts/kafka_local.sh start|topic|offsets' >&2; exit 2 ;;
esac
