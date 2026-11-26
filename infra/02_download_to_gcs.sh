#!/usr/bin/env bash
# Bronze: download raw files from Hugging Face into the bucket.
#
# Run this ON A VM, not in Cloud Shell (Cloud Shell has ~5 GB disk and was
# ~1.6 MB/s; the VM got ~100 MB/s). Create the VM first, from Cloud Shell:
#   gcloud compute instances create dl-vm --zone=us-central1-a \
#     --machine-type=e2-standard-2 --boot-disk-size=100GB --scopes=cloud-platform
#   gcloud compute ssh dl-vm --zone=us-central1-a
# Delete it when done:
#   gcloud compute instances delete dl-vm --zone=us-central1-a
#
# Notes from the first run:
#  - The old UCSD links (datarepo.eng.ucsd.edu/.../*.jsonl.gz) return 404. Use Hugging Face.
#  - Hugging Face files are plain .jsonl (not gzipped).
#  - Streaming curl | gcloud cp dropped mid-file (HTTP/2 error 92), so download to
#    disk with --http1.1 and resume (-C -), then upload.
set -euo pipefail
source "$(dirname "$0")/config.sh"

get() {   # $1 = path on Hugging Face, $2 = folder in bucket
  f=$(basename "$1")
  until curl -fL --http1.1 -C - -o "$f" "$HF/$1"; do echo "dropped, resuming..."; sleep 5; done
  gcloud storage cp "$f" "$BUCKET/raw/$2/$f" && rm "$f"
}

for C in All_Beauty Beauty_and_Personal_Care Health_and_Household; do
  get "review_categories/$C.jsonl" reviews
  get "meta_categories/meta_$C.jsonl" meta
done

gcloud storage ls -l "$BUCKET/raw/reviews/" "$BUCKET/raw/meta/"
gcloud storage du --summarize --readable-sizes "$BUCKET/raw/"
