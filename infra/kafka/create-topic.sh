#!/bin/bash
set -euo pipefail

BS=kafka-1:9092,kafka-2:9092,kafka-3:9092
T=/opt/kafka/bin

$T/kafka-topics.sh --bootstrap-server "$BS" --create --if-not-exists \
  --topic movie-events \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

$T/kafka-topics.sh --bootstrap-server "$BS" --describe --topic movie-events
