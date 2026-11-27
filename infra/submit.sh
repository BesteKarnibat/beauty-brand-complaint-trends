#!/usr/bin/env bash
# Submit a Spark job:  infra/submit.sh spark/eda_metrics.py
set -euo pipefail
source "$(dirname "$0")/config.sh"
gcloud dataproc jobs submit pyspark "$1" --cluster="$CLUSTER" --region="$REGION" --project="$PROJECT"
