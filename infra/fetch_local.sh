#!/usr/bin/env bash
# Copy the small outputs to local/ (git-ignored) so DuckDB can query them.
set -euo pipefail
source "$(dirname "$0")/config.sh"
# gcloud 567 crashes (TypeError) on sliced/parallel downloads of the bigger Parquet files
# and then hangs. Download each file whole, one at a time (only for this script).
export CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD=0
export CLOUDSDK_STORAGE_PROCESS_COUNT=1 CLOUDSDK_STORAGE_THREAD_COUNT=1
mkdir -p local/check
gcloud storage cp -r --project="$PROJECT" "$BUCKET/marts" local/
gcloud storage cp -r --project="$PROJECT" "$BUCKET/results" local/
gcloud storage rsync -r --project="$PROJECT" "$BUCKET/parquet/reviews/category=All_Beauty" local/check/category=All_Beauty
