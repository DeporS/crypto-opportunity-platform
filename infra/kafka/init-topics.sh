#!/bin/sh
set -e

echo "Waiting for Kafka..."

until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Kafka not ready yet..."
  sleep 2
done

echo "Creating Kafka topics..."

kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic raw.trades \
  --partitions 3 \
  --replication-factor 1

kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic dlq.trades \
  --partitions 3 \
  --replication-factor 1

echo "Kafka topics created successfully."

kafka-topics --bootstrap-server kafka:29092 --list