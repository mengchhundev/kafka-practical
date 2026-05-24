#!/bin/bash
set -e

PROPS=/opt/kafka/config/kraft/server.properties

# Format storage only on first start; skip on container restarts
if [ ! -f "${KAFKA_LOG_DIRS}/meta.properties" ]; then
    CLUSTER_ID=$(kafka-storage.sh random-uuid)
    kafka-storage.sh format -t "$CLUSTER_ID" -c "$PROPS"
fi

exec kafka-server-start.sh "$PROPS"
