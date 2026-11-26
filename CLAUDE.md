# CLAUDE.md: rules for this repo

## What this is
Final project for ADSP 32030. Finds beauty brands whose share of 1–2 star Amazon reviews
is rising faster than their category, and explains why. Read `docs/PROGRESS.md` first:
it has the status, past problems, and the next steps in order.

## Where things are
- Cloud: GCP project `adsp32030-finalproject`, region `us-central1`.
- Bucket: `gs://adsp32030-amazon-reviews`
  - `raw/` Bronze (never modify) · `parquet/` Silver · `marts/`, `results/` Platinum
- Settings: `infra/config.sh` (source it before any gcloud command).
- Spark jobs: `spark/` · DuckDB queries: `queries/` · docs: `docs/`.
- `local/` holds copies for DuckDB and is git-ignored.

## How to run
- Spark: `infra/cluster_create.sh` → `infra/submit.sh spark/<job>.py` → `infra/cluster_delete.sh`.
  Dataproc image 2.2 (Spark 3.5), 1 master + 2 n2-standard-4 workers.
- DuckDB: `infra/fetch_local.sh`, then `python queries/run.py queries/<file>.sql`.
- See `.claude/skills/spark-dataproc-job/SKILL.md` for the full job checklist.

## Rules
- **Never** modify or delete anything under `raw/` in the bucket.
- **Never** commit data (`data/`, `local/`, Parquet, JSONL) or secrets (`.env`, keys).
  Small aggregate CSVs in `results/` are fine.
- **Always delete the Dataproc cluster** when a job is done, and confirm with
  `gcloud dataproc clusters list` and `gcloud compute instances list`.
- Ask before creating anything that costs money (VMs, clusters, Atlas tiers).
- Show me the diff before running a job, and the job output after.
- For any Spark change, explain what crosses the shuffle and why.
- Give reviews an explicit schema; never let Spark infer it over the full data.
- Write marts with `.coalesce(n)`: the brand mart once came out as 1,001 small files.
- Every new number needs a check: a second engine (DuckDB vs Spark, SQL vs MQL) or a
  known count. Put the check in the job output.

## Known numbers (regression checks)
- Rows: All_Beauty 701,528 · Beauty_and_Personal_Care 23,911,390 ·
  Health_and_Household 25,631,345 · products 1,939,067 (before dedupe).
- Duplicate review keys: 495,107 total (7,276 / 242,535 / 245,296).
- Beauty low-star rate: 2019 0.15228 · 2022 0.21277 (before dedupe).
- TecUnite low-star 2019 17.3% → 2022 63.4%.

## Gotchas already hit
- UCSD download links are 404; use Hugging Face (`infra/config.sh` → `$HF`).
- The compute service account needs `storage.objectAdmin` on the bucket and
  `dataproc.worker` on the project; grant from Cloud Shell as the owner.
- Long heredocs pasted into Cloud Shell can get scrambled; check with
  `tail` and `python3 -m py_compile` before submitting.
- `| head` after `gcloud storage cat` prints a harmless hash-mismatch error.

## Style
- Python 3, PySpark DataFrame API, short docstring at the top of each job saying
  what layer it builds, what it outputs, and how long it took.
- Plain English in docs; numbers over adjectives.
