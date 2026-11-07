#!/usr/bin/env bash
# Start the Dagster UI and daemon for this project (http://localhost:3000).
set -euo pipefail
cd "$(dirname "$0")/.."
export DAGSTER_HOME="$PWD/dagster_home"
exec dagster dev -m lakehouse.orchestration