#!/usr/bin/env bash
set -e

docker compose exec -it kafka1 \
    kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server localhost:29092 \
        --topic sensor-alerts \
        --time -1
