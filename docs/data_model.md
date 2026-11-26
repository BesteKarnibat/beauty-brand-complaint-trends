# Data model

## Layers

| Layer | What happens | Where it lives | Checks |
|---|---|---|---|
| Bronze | Raw JSONL from Hugging Face, never edited | `raw/reviews/`, `raw/meta/` (26.3 GiB) | File sizes match Hugging Face; last line of each file is complete JSON |
| Silver | Explicit schema; ms → timestamp; only needed product fields; partitioned by category and year | `parquet/reviews/`, `parquet/meta/` | All_Beauty 701,528 rows = dataset site; every product in exactly one category. **To do:** dedupe |
| Gold | Star schema | joined inside `eda_metrics.py` (**to be saved**) | Join keeps 701,528 rows (no fan-out); 100% of reviews find a product; 97.96% find a brand |
| Platinum | Low-star rate per brand × category × year; category baseline | `marts/brand_year_metrics/`, `results/category_year/` | Spark = DuckDB on all 24 years |

All in one bucket (`gs://adsp32030-amazon-reviews`), one region (`us-central1`).

## Star schema

| Table | One row = | Key | Joins to | Status |
|---|---|---|---|---|
| fact_review | one review (50.2M) | user_id + parent_asin + timestamp | dim_product, dim_date, dim_category | Built (Silver) |
| dim_product | one product | parent_asin | brand_alias | Built (Silver) |
| brand_alias | one raw brand spelling | brand_raw | dim_brand | Planned |
| dim_brand | one cleaned brand | brand_sk (hash of brand_key) | parent company, price tier | Planned |
| dim_date | one day | review_date | year, month | Built as columns |
| dim_category | one category file | category | beauty or baseline | Built as a column |
| complaint_label | one sampled review | review key (MongoDB) | fact_review | Planned |
| brand_year_metrics | brand × category × year | brand_key + category + year | category_year | Built |
| category_year | category × year | category + year | brand_year_metrics | Built |

**How it answers the question:** `brand_year_metrics` minus `category_year` = how much
faster a brand got worse than its category. `complaint_label` adds *what* the complaint was.

## Field notes

- **Brand** = `coalesce(details.Brand, store)`, then `brand_key` = lowercase, accents
  translated, non-alphanumerics removed. Groups "Rëvlóñ" with "Revlon", but not
  "L'Oréal Paris" with "L'Oréal" (that needs `brand_alias`).
- **low_star** = rating ≤ 2. A stand-in for "complaint" until Claude labels reviews.
- **Review key is not unique:** ~1.0% repeats. Dedupe in Silver.
- Review text stays in Parquet for Spark; only sampled reviews go to MongoDB.
