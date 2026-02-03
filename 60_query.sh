#!/usr/bin/env bash
set -e

docker compose exec -it clickhouse1 \
    clickhouse-client --queries-file /scripts/query.sql \
        --echo \
        --verbose \
        --format PrettyCompact
