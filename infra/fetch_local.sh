#!/usr/bin/env bash
# Copy the small outputs to local/ (git-ignored) so DuckDB can query them.
# Mirrors GCS: local files that are gone from the bucket are deleted, because every Spark
# rerun writes new file names and old copies would be counted twice.
set -euo pipefail
source "$(dirname "$0")/config.sh"
# gcloud 567 crashes (TypeError) on sliced/parallel downloads of the bigger Parquet files
# and then hangs. Download each file whole, one at a time (only for this script).
export CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD=0
export CLOUDSDK_STORAGE_PROCESS_COUNT=1 CLOUDSDK_STORAGE_THREAD_COUNT=1
mkdir -p local/marts local/results local/check local/gold/fact_review
gcloud storage rsync -r --delete-unmatched-destination-objects --project="$PROJECT" "$BUCKET/marts" local/marts
gcloud storage rsync -r --delete-unmatched-destination-objects --project="$PROJECT" "$BUCKET/results" local/results
gcloud storage rsync -r --delete-unmatched-destination-objects --project="$PROJECT" "$BUCKET/parquet/reviews/category=All_Beauty" local/check/category=All_Beauty
# Gold fact_review, All_Beauty only (for queries/01_ask_twice_all_beauty.sql)
gcloud storage rsync -r --delete-unmatched-destination-objects --project="$PROJECT" "$BUCKET/gold/fact_review/category=All_Beauty" local/gold/fact_review/category=All_Beauty
