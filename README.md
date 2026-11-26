# Beauty Brand Complaint Trends

**Which beauty brands are losing customers, and why?**

Retail buyers and distributors decide which beauty brands to stock, reorder or promote.
Large brands pay for review analytics; smaller buyers only see an average star rating,
which hides a brand that is slowly getting worse.

This project uses **50.2 million Amazon reviews (26.3 GiB)** to find brands whose share of
1–2 star reviews is rising **faster than their category**, and then to explain *what*
customers are complaining about.

> UChicago MS in Applied Data Science · ADSP 32030 Scalable Data Systems & AI Pipelines ·
> Final project · Beste Karnibat

## The key idea

Low-star reviews rose about **6 points for everyone** from 2019 to 2022
(Beauty & Personal Care 15.2% → 21.3%, Health & Household 13.5% → 19.8%).
So a raw rise means little. Each brand is compared to its own category:

```
rise above category = (brand 2022 − brand 2019) − (category 2022 − category 2019)
```

## Architecture

```
Hugging Face (6 JSONL files)
   │  resumable curl on a VM
   ▼
Cloud Storage  gs://adsp32030-amazon-reviews   (one bucket, us-central1)
   raw/        Bronze   26.3 GiB, as delivered
   │  Spark on Dataproc
   ▼
   parquet/    Silver   typed, partitioned by category/year
   ▼
   (Gold)      star schema: fact_review + product/brand/date/category dims
   ▼
   marts/      Platinum brand × category × year rates, category baseline
   │
   ├──► DuckDB            analysis and "ask it twice" checks
   ├──► MongoDB Atlas     review documents + Atlas Search          (next)
   └──► Claude via MCP    complaint-type labels on a sample        (next)
```

## Data

[Amazon Reviews 2023](https://amazon-reviews-2023.github.io/) (McAuley Lab, UC San Diego),
three categories:

| Category | Reviews | Products | Role |
|---|---|---|---|
| Beauty_and_Personal_Care | 23,911,390 | 1,028,914 | Main category |
| Health_and_Household | 25,631,345 | 797,563 | Baseline (beauty brands also sell here) |
| All_Beauty | 701,528 | 112,590 | Test set and pipeline check |

**Right to use:** released for research; the lab assigns no formal license. Used here for
non-commercial coursework. Data is **not** stored in this repo. Cite: Hou et al., 2024,
*Bridging Language and Items for Retrieval and Recommendation*.

## Results so far

See [`docs/results_so_far.md`](docs/results_so_far.md). Headline: after removing the
category-wide rise, brands such as BTYMS (+20.4 pts), iMethod (+13.3) and several deodorant
and body-care brands (Crystal, Viking Revolution, Irish Spring, Mitchum) got worse fastest.

## Repo layout

| Path | What it is |
|---|---|
| `infra/` | GCP setup, download, cluster create/submit/delete scripts |
| `spark/` | Spark jobs run on Dataproc |
| `queries/` | DuckDB queries (the "meaty queries"), with a runner |
| `results/` | Small aggregate CSVs from Spark (no raw data) |
| `docs/` | Progress log, data model, data traps, results, check-in deck |
| `CLAUDE.md` | Rules for Claude Code in this repo |
| `.claude/skills/` | Skills that control how Claude runs jobs and labels reviews |

## How to run

```bash
source infra/config.sh
bash infra/01_setup_gcp.sh          # once: APIs, bucket, IAM
# Bronze: run infra/02_download_to_gcs.sh on a VM (see the comments in the file)
bash infra/cluster_create.sh
bash infra/submit.sh spark/to_parquet.py     # Bronze -> Silver
bash infra/submit.sh spark/eda_metrics.py    # Gold (in memory) -> Platinum
bash infra/submit.sh spark/eda_traps.py      # data-quality measurements
bash infra/cluster_delete.sh                 # always
bash infra/fetch_local.sh                    # small outputs -> local/
pip install -r requirements.txt
python queries/run.py queries/02_rise_above_category.sql
```

## Status

See [`docs/PROGRESS.md`](docs/PROGRESS.md) for what is done, what went wrong and how it was
fixed, and the plan to December 11.

## Using AI agents

Claude (chat, then Claude Code) helped design the pipeline, write the Spark jobs and build
the check-in deck. The rules it follows are in [`CLAUDE.md`](CLAUDE.md) and
[`.claude/skills/`](.claude/skills/). I reviewed and ran every job myself; every number
here was checked against an independent count or a second engine.
