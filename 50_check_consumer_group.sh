#!/usr/bin/env bash
set -e

docker compose exec -it kafka1 \
    kafka-consumer-groups \
        --bootstrap-server localhost:29092 \
        --describe --group clickhouse-2
