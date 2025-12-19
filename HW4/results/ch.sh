#!/usr/bin/env bash
set -euo pipefail

SERVICE="${CH_SERVICE:-clickhouse1}"

docker compose exec -T "${SERVICE}" clickhouse-client "$@"


