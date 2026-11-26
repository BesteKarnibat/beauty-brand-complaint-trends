#!/usr/bin/env bash
# Always run this when you are done. The bucket (your data) is NOT affected.
set -euo pipefail
source "$(dirname "$0")/config.sh"
gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" --quiet
gcloud dataproc clusters list --region="$REGION"
gcloud compute instances list
