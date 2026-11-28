# Queries

Each file is one "meaty query" for the final project. Run from the repo root after
`infra/fetch_local.sh`:

```bash
python queries/run.py queries/02_rise_above_category.sql
```

| # | File | Question | Status |
|---|------|----------|--------|
| 1 | `01_ask_twice_all_beauty.sql` | Do DuckDB and Spark agree (on the clean Gold data)? | Done: all 24 years match |
| 2 | `02_rise_above_category.sql` | Which brands got worse faster than their category? | Done |
| check | `check_dedupe_all_beauty.sql` | Does DuckDB's dedupe match Spark's? | Done: all 24 years match |
| 3 | `03_complaints_vs_share.sql` | Did rising-complaint brands lose share of reviews? | Done: not in the same window; yes, one year later (corr -0.17) |
| 4 | (planned) | Complaint mix per brand, 2019 vs 2022 (MongoDB aggregation on Claude labels) | To do |
| 5 | (planned) | Atlas Search: "burning / rash / leaked / new formula" by brand and year | To do |
| 6 | (planned) | Reviewer concentration and same-day 5-star bursts | To do |
| 7 | (planned) | Same number in SQL (DuckDB) and MQL (MongoDB) | To do |
