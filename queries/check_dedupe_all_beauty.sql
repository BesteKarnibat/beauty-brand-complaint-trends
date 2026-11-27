-- Ask it twice, after dedupe: DuckDB removes the duplicate reviews from the All_Beauty
-- Silver copy on its own, then compares per-year counts and low-star rates with
-- Spark's deduped results/category_year (written by spark/build_gold.py).
-- Same key and same tie-break as Spark: keep the copy with the most helpful votes, then
-- verified, then higher rating, longer text, asin, later time (NULLs lowest, as in Spark).
-- Needs the full All_Beauty Silver copy in local/check/ (701,528 rows, 24 years).
-- Last row = totals: 701,528 Silver rows should become 694,252 after dedupe (7,276 removed).
-- Result (Nov 26): all 24 years match Spark exactly (counts and rates); 694,252 = 694,252.
WITH silver AS (
  SELECT * FROM read_parquet('local/check/category=All_Beauty/*/*.parquet',
                             hive_partitioning = true)),
dedup AS (
  SELECT * FROM silver
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, parent_asin, "timestamp"
    ORDER BY helpful_vote DESC NULLS LAST, verified_purchase DESC NULLS LAST,
             rating DESC NULLS LAST, length(text) DESC NULLS LAST,
             asin DESC NULLS LAST, review_ts DESC NULLS LAST) = 1),
duck AS (
  SELECT year, COUNT(*) AS n_reviews,
         AVG(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS low_star_rate
  FROM dedup GROUP BY year),
spark AS (
  SELECT year, n_reviews, low_star_rate
  FROM read_csv_auto('local/results/category_year/*.csv')
  WHERE category = 'All_Beauty')
SELECT COALESCE(d.year, s.year)                 AS year,
       d.n_reviews                              AS duckdb_n,
       s.n_reviews                              AS spark_n,
       ROUND(d.low_star_rate, 6)                AS duckdb_rate,
       ROUND(s.low_star_rate, 6)                AS spark_rate,
       d.n_reviews = s.n_reviews
         AND ABS(d.low_star_rate - s.low_star_rate) < 1e-9 AS match
FROM duck d FULL OUTER JOIN spark s ON d.year = s.year
UNION ALL
SELECT NULL, (SELECT COUNT(*) FROM dedup), (SELECT SUM(n_reviews) FROM spark),
       NULL, NULL,
       (SELECT COUNT(*) FROM dedup) = 694252 AND (SELECT COUNT(*) FROM silver) = 701528
ORDER BY "year" NULLS LAST;
