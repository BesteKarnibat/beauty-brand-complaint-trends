# Shared settings. Source this first:  source infra/config.sh
export PROJECT=adsp32030-finalproject
export REGION=us-central1
export ZONE=us-central1-a
export BUCKET=gs://adsp32030-amazon-reviews
export CLUSTER=reviews-cluster
# Compute Engine default service account (used by the VM and by Dataproc)
export COMPUTE_SA=909556074519-compute@developer.gserviceaccount.com
export HF=https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw
