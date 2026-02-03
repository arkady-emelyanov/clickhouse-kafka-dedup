#!/usr/bin/env bash
set -e

docker compose exec -it kafka1 \
    kafka-topics --bootstrap-server localhost:29092 --create \
        --topic "sensor-alerts" \
        --partitions 6 \
        --replication-factor 1
