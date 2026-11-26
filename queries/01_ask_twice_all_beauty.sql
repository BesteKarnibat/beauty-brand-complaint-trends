-- Ask it twice: recompute All_Beauty reviews and low-star rate per year in DuckDB
-- and compare with Spark's results/category_year.
-- Result (Nov 16): all 24 years match Spark exactly, e.g. 2021 = 124,897 reviews, 0.26009.
SELECT year,
       COUNT(*)                                     AS n_reviews,
       AVG(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS low_star_rate
FROM read_parquet('local/check/category=All_Beauty/*/*.parquet', hive_partitioning = true)
GROUP BY year
ORDER BY year;
