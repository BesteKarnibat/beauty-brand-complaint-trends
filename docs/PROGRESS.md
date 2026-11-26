# Progress log

What has been done so far, what went wrong and how it was fixed, and what is next.
Last updated: November 25, 2026.

## Status at a glance

| Piece | Status |
|---|---|
| Bronze: raw JSONL in GCS (26.3 GiB) | ✅ Done |
| Silver: typed Parquet, partitioned by category/year | ✅ Done (dedupe still to do) |
| Gold: star schema tables saved | ⏳ Joined in memory only; save as tables |
| Platinum: brand_year_metrics, category_year | ✅ Done (rerun after dedupe) |
| DuckDB checks ("ask it twice") | ✅ Done |
| brand_alias / dim_brand | ⏳ To do |
| MongoDB Atlas + Atlas Search | ⏳ To do |
| Claude complaint labeling (MongoDB MCP) | ⏳ To do |
| Outcome: share of reviews vs complaints | ⏳ To do |
| Reviewer / graph analysis | ⏳ To do |
| Live or incremental piece | 💡 Stretch |
| Dashboard, recording, repo walkthrough | ⏳ Final week |

## Grades so far

- Proposal: 9/10. Missing: name, "how" paragraph, three tools, two placeholders (size, license).
- Check-in: 10/10 + 5 extra credit for presenting in class (15/15).

## Timeline

### Nov 15–16: data in, first results, check-in deck

1. **Download.** The UCSD links on the dataset site return 404. Switched to the
   Hugging Face copy (`McAuley-Lab/Amazon-Reviews-2023`, plain `.jsonl`).
2. **Cloud Shell was too slow** (1.6 MB/s) and the stream dropped (curl error 92,
   HTTP/2). Moved to a VM (`e2-standard-2`, 100 GB disk): ~100 MB/s, download to
   disk with `--http1.1 -C -` (resume), then upload. VM deleted after.
3. **IAM.** The VM and Dataproc run as the compute service account, which had no
   access. Fixed by granting `roles/storage.objectAdmin` on the bucket and
   `roles/dataproc.worker` on the project (from Cloud Shell, as the owner;
   the service account cannot grant itself roles).
4. **Bronze landed:** 3 review files (21.18 GiB) + 3 metadata files (5.14 GiB).
5. **`spark/to_parquet.py`** (~5 min): 50,244,263 reviews, 1,939,067 products.
   All_Beauty = 701,528 rows, matching the 701.5K on the dataset site.
6. **`spark/eda_metrics.py`** (~4 min): brand mart and result tables.
7. **DuckDB check:** All_Beauty per-year counts and rates match Spark on all 24 years.
8. **`spark/eda_traps.py`** (~2 min): measured seven data traps (see `data_traps.md`).
9. **Check-in deck:** `docs/checkin/Checkin_BesteKarnibat.pptx` (15 slides).

### Nov 17: check-in presented in class

Feedback from Prof. Barry, things to consider:
- Revenue and outcomes: did brands with rising complaints lose share?
- Graph angle: who drives the reviews? Dominant reviewers, possibly paid reviews.
- FDA adverse events as a second source to validate against.
- Save the Gold tables, add brand_alias, get Atlas + Claude labeling running.
- Something live or incremental.

## What the final (Dec 11) must have

- A running system, end to end, in the cloud, using at least three course platforms.
- Big data (more than one computer can handle) or live/streaming through Spark.
- At least 5 meaty queries in the repo; show some with results in the recording.
- Findings and conclusions that close the loop on the problem.
- Story order: problem → how → data sources → architecture → layers and model →
  analysis → conclusions → demo. About 10 minutes, under 15.
- Agent instructions in the repo (`CLAUDE.md`, `.claude/skills/`).
- GitHub repo (public), the recording, and a zip of the materials. End with a repo walkthrough.

## Next steps (in order)

### Week of Nov 25 (Thanksgiving, light)
- [ ] Spark job `spark/build_gold.py`:
  - dropDuplicates on (user_id, parent_asin, timestamp) in Silver
  - brand_alias for the top 100 beauty brands (e.g. L'Oréal Paris → L'Oréal)
  - save Gold tables: fact_review, dim_product, dim_brand, dim_category, dim_date
  - rebuild Platinum with `.coalesce()` (fix the 1,001 small files)
  - add share of category reviews per brand-year (outcome proxy)
- [ ] Rerun query 02 on the clean data; record any changes.
- [ ] Query 03: complaints vs share of reviews.

### Week of Dec 1
- [ ] MongoDB Atlas: load reviews for ~10 flagged brands (sample; free tier is small).
- [ ] Atlas Search index on review text; query 05.
- [ ] Claude labeling of ~1,000 sampled reviews into complaint types; hand-check 100–200.
- [ ] Query 04 (complaint mix) and 07 (same number in SQL and MQL).
- [ ] Reviewer analysis in Spark (query 06).

### Week of Dec 7
- [ ] Streamlit dashboard: brand vs category trend, complaint mix, example reviews.
- [ ] Optional: replay 2023 reviews through Pub/Sub as a live feed.
- [ ] Record the video, write the README results, zip the materials, submit early.
