#!/usr/bin/env bash
# Start a small Dataproc cluster. It deletes itself after 1 hour idle.
# If you hit a CPU quota error, change both machine types to n2-standard-2.
set -euo pipefail
source "$(dirname "$0")/config.sh"
gcloud dataproc clusters create "$CLUSTER" --region="$REGION" --project="$PROJECT" \
  --master-machine-type=n2-standard-4 --worker-machine-type=n2-standard-4 \
  --num-workers=2 --image-version=2.2-debian12 --max-idle=1h
