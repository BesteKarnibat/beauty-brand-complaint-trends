-- First result: which beauty brands' low-star rate rose faster than the category's,
-- 2019 -> 2022. Brands need >= 1,000 reviews in both years.
-- The category itself rose 6.05 points (15.2% -> 21.3%).
-- Result (Nov 16): TecUnite +40.0, BTYMS +20.4, iMethod +13.3, EAONE +11.7, LKE +11.0 ...
WITH cat AS (
  SELECT year, low_star_rate AS cat_rate
  FROM read_csv_auto('local/results/category_year/*.csv')
  WHERE category = 'Beauty_and_Personal_Care'),
b AS (
  SELECT * FROM read_parquet('local/marts/brand_year_metrics/*.parquet')
  WHERE category = 'Beauty_and_Personal_Care' AND n_reviews >= 1000)
SELECT b19.brand_example AS brand,
       ROUND(100 * b19.low_star_rate, 1) AS pct_2019,
       ROUND(100 * b22.low_star_rate, 1) AS pct_2022,
       ROUND(100 * ((b22.low_star_rate - c22.cat_rate) - (b19.low_star_rate - c19.cat_rate)), 1)
         AS rise_above_category_pts
FROM b b19
JOIN b b22 ON b19.brand_key = b22.brand_key AND b19.year = 2019 AND b22.year = 2022
JOIN cat c19 ON c19.year = 2019
JOIN cat c22 ON c22.year = 2022
ORDER BY rise_above_category_pts DESC
LIMIT 15;
