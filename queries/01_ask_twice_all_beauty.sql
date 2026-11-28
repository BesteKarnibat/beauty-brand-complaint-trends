-- Ask it twice: recompute All_Beauty reviews and low-star rate per year in DuckDB, straight
-- from Spark's clean Gold table (gold/fact_review, duplicates removed), and compare with
-- Spark's results/category_year. Checks the aggregation; check_dedupe_all_beauty.sql
-- checks the dedupe itself.
-- Result (Nov 16, raw Silver, before dedupe): all 24 years matched, e.g. 2021 = 124,897, 0.26009.
-- Result (Nov 27, Gold, after dedupe): all 24 years match (counts, low-star and verified-only
-- rates), 694,252 reviews in both, e.g. 2021 = 123,512, 0.26011.
WITH duck AS (
  SELECT year,
         COUNT(*)                                          AS n_reviews,
         AVG(CASE WHEN rating <= 2 THEN 1 ELSE 0 END)      AS low_star_rate,
         AVG(low_star) FILTER (WHERE verified_purchase)    AS low_star_rate_verified
  FROM read_parquet('local/gold/fact_review/category=All_Beauty/*/*.parquet',
                    hive_partitioning = true)
  GROUP BY year),
spark AS (
  SELECT year, n_reviews, low_star_rate, low_star_rate_verified
  FROM read_csv_auto('local/results/category_year/*.csv')
  WHERE category = 'All_Beauty')
SELECT COALESCE(d.year, s.year)        AS year,
       d.n_reviews                     AS duckdb_n,
       s.n_reviews                     AS spark_n,
       ROUND(d.low_star_rate, 5)       AS duckdb_rate,
       ROUND(s.low_star_rate, 5)       AS spark_rate,
       d.n_reviews = s.n_reviews
         AND ABS(d.low_star_rate - s.low_star_rate) < 1e-9
         -- 2000-2001 have no verified reviews: both sides are NULL, which counts as a match
         AND COALESCE(ABS(d.low_star_rate_verified - s.low_star_rate_verified) < 1e-9,
                      d.low_star_rate_verified IS NULL AND s.low_star_rate_verified IS NULL) AS match
FROM duck d FULL OUTER JOIN spark s ON d.year = s.year
ORDER BY 1
