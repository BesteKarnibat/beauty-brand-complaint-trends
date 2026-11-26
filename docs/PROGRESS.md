# Progress log

What has been done so far, what went wrong and how it was fixed, and what is next.
Last updated: November 26, 2026.

## Status at a glance

| Piece | Status |
|---|---|
| Bronze: raw JSONL in GCS (26.3 GiB) | ✅ Done |
| Silver: typed Parquet, partitioned by category/year | ✅ Done (dedupe happens in Gold) |
| Gold: star schema tables saved | ✅ Done (`gold/`, Nov 26) |
| Platinum: brand_year_metrics, category_year | ✅ Rebuilt on deduped data, 4 files |
| DuckDB checks ("ask it twice") | ✅ Done, incl. dedupe (24/24 years) |
| brand_alias / dim_brand | ✅ Top 100, 44 merges; L'Oréal Professionnel split out |
| MongoDB Atlas + Atlas Search | ⏳ To do |
| Claude complaint labeling (MongoDB MCP) | ⏳ To do |
| Outcome: share of reviews vs complaints | 🟡 share column built; query 03 to do |
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

### Nov 25–26: Gold tables saved (`spark/build_gold.py`, ~9 min)

1. **Logged in from the laptop** (`gcloud auth login`, then the scripts in `infra/` now pass
   `--project`, because the laptop's default project was a different one).
2. **Dedupe** on (user_id, parent_asin, timestamp), keeping the copy with the most helpful
   votes: 50,244,263 → 49,749,156 rows. Removed exactly the 495,107 measured earlier
   (7,276 / 242,535 / 245,296). Only 4 duplicate keys had copies with different ratings,
   so the copies are almost all exact repeats.
3. **Gold saved** under `gold/`: fact_review (no text, `text_len` only), dim_product,
   brand_alias, dim_brand, dim_category, dim_date.
4. **brand_alias**: top 100 beauty brands; a manual map plus "top brand + known ending"
   (paris, men, professional, cosmetics...). 44 spellings merged, e.g. L'Oréal Paris
   (273,250 reviews) → loreal, Maybelline New York → maybelline, Nivea Men → nivea,
   Wahl Professional → wahl. L'Oréal Professionnel (salon line, 1,757 reviews) kept as
   its own brand after a hand check; rerun took L'Oréal from 275,505 to 273,748 reviews.
   List in `results/brand_alias_merges/`.
5. **Platinum rebuilt**: brand_year_metrics is now 4 files (was 1,001), with
   `share_of_category_reviews`, a verified-only low-star rate and the gap to the category.
6. **Effect of dedupe** is small: Beauty low-star 2019 0.15228 → 0.15215,
   2022 0.21277 → 0.21243; TecUnite 2019 17.3% → 17.5%, 2022 63.4% unchanged.
7. **Checks**: all 21 passed (`results/gold_checks/`), including share of category
   = Spark SQL on the Gold tables for the top 5 brands. Cluster deleted, 0 clusters, 0 VMs.

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
- [x] Spark job `spark/build_gold.py` (Nov 26):
  - dropDuplicates on (user_id, parent_asin, timestamp) in Silver
  - brand_alias for the top 100 beauty brands (e.g. L'Oréal Paris → L'Oréal)
  - save Gold tables: fact_review, dim_product, dim_brand, dim_category, dim_date
  - rebuild Platinum with `.coalesce()` (fix the 1,001 small files)
  - add share of category reviews per brand-year (outcome proxy)
- [ ] Finish the hand check of `results/brand_alias_merges/` (L'Oréal Professionnel done; Philips Norelco still separate).
- [x] DuckDB check of the deduped All_Beauty counts (Nov 26, `queries/check_dedupe_all_beauty.sql`):
      DuckDB dedupes on its own and matches Spark on all 24 years; 694,252 rows in both.
- [x] Rerun query 02 on the clean data (Nov 26): same brands, rates within 0.5 pts; TecUnite
      drops out only because 2022 fell to 998 reviews (cutoff 1,000). See `results_so_far.md`.
- [x] `infra/fetch_local.sh` hung on the All_Beauty copy: gcloud 567 crashes (TypeError) on sliced
      downloads of the 7 bigger files and then hangs, leaving a partial copy (85,688 of 701,528 rows).
      Fixed by turning sliced/parallel downloads off in the script.
- [ ] Query 01 compares raw Silver with `category_year`, which is now deduped; update it or retire it.
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
