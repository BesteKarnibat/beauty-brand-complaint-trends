---
name: spark-dataproc-job
description: Use when writing, changing or running any Spark job in spark/ for this project on Google Cloud Dataproc. Covers the create-submit-verify-delete loop, schema and partitioning rules, and the checks every job must print.
---

# Running a Spark job on Dataproc in this repo

## Before writing code
1. Read `docs/PROGRESS.md` and `docs/data_model.md` to see which layer the job builds.
2. Say in one sentence what the job reads, what it writes, and what crosses the shuffle.

## Writing the job
- Top docstring: layer (Bronze/Silver/Gold/Platinum), inputs, outputs, expected runtime.
- Bucket constant: `B = "gs://adsp32030-amazon-reviews/"`.
- Read Silver from `parquet/reviews` (partitioned by `category`, `year`) and `parquet/meta`.
- Select only the columns needed before a join (the review text is most of the bytes).
- Review key = (`user_id`, `parent_asin`, `timestamp`). It is **not unique**: dedupe first.
- Group on `parent_asin`, never `asin` (51.6% of beauty reviews are on variants).
- Write outputs with `mode("overwrite")` so reruns are idempotent; `.coalesce(n)` small outputs.
- End the job by printing its checks (row counts, a known number, before/after counts).

## Running it
```bash
python3 -m py_compile spark/<job>.py
source infra/config.sh
gcloud dataproc clusters list --region=$REGION     # create only if none is running
bash infra/cluster_create.sh
bash infra/submit.sh spark/<job>.py
bash infra/cluster_delete.sh                        # always, even if the job failed
```

## After it runs
- Compare printed checks with "Known numbers" in `CLAUDE.md`.
- Recompute one key number in DuckDB (`queries/`) and confirm it matches.
- Update `docs/PROGRESS.md` (what ran, runtime, results, issues).
