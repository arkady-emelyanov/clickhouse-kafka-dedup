#!/usr/bin/env bash
set -e

echo "Resetting ClickHouse schema on cluster 'my_cluster'..."
docker compose exec -it clickhouse1 \
    clickhouse-client --queries-file /scripts/schema_drop.sql

echo "Waiting for 10 seconds to ensure detachment is complete..."
sleep 10

echo "Resetting Kafka consumer group 'clickhouse-4' offsets to earliest..."
docker compose exec -it kafka1 \
    kafka-consumer-groups --bootstrap-server localhost:29092 \
        --group clickhouse-4 \
        --topic events \
        --reset-offsets \
        --to-earliest \
        --execute

echo "Creating ClickHouse schema on cluster 'my_cluster'..."
docker compose exec -it clickhouse1 \
    clickhouse-client --queries-file /scripts/schema_create.sql
