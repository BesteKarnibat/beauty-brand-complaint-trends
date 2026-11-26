#!/usr/bin/env bash
# One-time setup. Run in Cloud Shell, logged in as yourself (project owner).
set -euo pipefail
source "$(dirname "$0")/config.sh"

gcloud config set project "$PROJECT"
gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com

# Bucket for everything: raw/ (Bronze), parquet/ (Silver), marts/ + results/ (Platinum)
gcloud storage buckets create "$BUCKET" --location="$REGION" || true

# The VM and Dataproc both run as the compute service account.
# New projects don't give it these roles, so grant them once:
gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:$COMPUTE_SA" --role=roles/storage.objectAdmin
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$COMPUTE_SA" --role=roles/dataproc.worker
