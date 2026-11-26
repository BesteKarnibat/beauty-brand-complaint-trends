#!/usr/bin/env bash
# Copy the small outputs to local/ (git-ignored) so DuckDB can query them.
set -euo pipefail
source "$(dirname "$0")/config.sh"
mkdir -p local/check
gcloud storage cp -r "$BUCKET/marts" local/
gcloud storage cp -r "$BUCKET/results" local/
gcloud storage cp -r "$BUCKET/parquet/reviews/category=All_Beauty" local/check/
