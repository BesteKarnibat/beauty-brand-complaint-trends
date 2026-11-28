-- Did brands whose complaints rose faster than their category lose share of reviews?
-- Beauty_and_Personal_Care brands with >= 1,000 reviews in both 2019 and 2022 (same set as 02).
--   complaint rise = change in (brand low-star rate - category low-star rate), 2019 -> 2022, points
--   share change   = relative change in the brand's share of all category reviews, in %
-- Share is compared in the same window (2019 -> 2022) and one year later (2022 -> 2023).
-- 2023 ends in September; a share is a fraction of that year's reviews, so it still compares,
-- but seasonal brands (e.g. sunscreen) can be off. Reviews stand in for sales.
-- Most big brands lose share over time as small new brands enter, so compare the groups
-- with each other, not with zero.
-- Rows 1-4 = complaint-rise quarters; row 5 = all brands, with correlations and checks:
--   corr_*            Pearson correlation of complaint rise with share change
--   spearman_next_yr  same on ranks (not driven by a few extreme brands)
--   t_next_yr         t statistic for corr_next_yr (|t| > 1.97 = p < 0.05 at n = 259)
--   check_share_ok    share_of_category_reviews from Spark = n_reviews / cat_n_reviews
-- Result (Nov 27, 259 brands): no link in the same window (corr -0.006; every quarter lost
--   24-30% of its share). One year later, the two quarters with the biggest complaint rise lost
--   about twice as much share (median -22.5% and -20.1%) as the two lowest (-10.8%, -9.1%);
--   corr -0.174, Spearman -0.171, t = -2.84 (p < 0.01). Same numbers recomputed in plain
--   Python. The effect is real but small: complaint rise explains ~3% of the variation.
WITH b AS (
  SELECT * FROM read_parquet('local/marts/brand_year_metrics/*.parquet')
  WHERE category = 'Beauty_and_Personal_Care'),
p AS (
  SELECT b19.brand_key,
         100 * ((b22.low_star_rate - b22.cat_low_star_rate)
              - (b19.low_star_rate - b19.cat_low_star_rate))                 AS rise,
         100 * (b22.share_of_category_reviews / b19.share_of_category_reviews - 1) AS share_same,
         100 * (b23.share_of_category_reviews / b22.share_of_category_reviews - 1) AS share_next
  FROM b b19
  JOIN b b22 ON b22.brand_key = b19.brand_key AND b19.year = 2019 AND b22.year = 2022
  LEFT JOIN b b23 ON b23.brand_key = b19.brand_key AND b23.year = 2023
  WHERE b19.n_reviews >= 1000 AND b22.n_reviews >= 1000),
q AS (
  SELECT *, NTILE(4) OVER (ORDER BY rise) AS quarter,
         RANK() OVER (ORDER BY rise) AS r_rise, RANK() OVER (ORDER BY share_next) AS r_next
  FROM p),
stats AS (
  SELECT CORR(rise, share_same) AS c_same, CORR(rise, share_next) AS c_next,
         CORR(r_rise, r_next) AS s_next, COUNT(share_next) AS n_next
  FROM q),
chk AS (
  SELECT MAX(ABS(share_of_category_reviews - n_reviews / cat_n_reviews)) < 1e-12 AS ok FROM b)
SELECT CAST(quarter AS VARCHAR)                         AS complaint_rise_group,
       COUNT(*)                                        AS n_brands,
       ROUND(MIN(rise), 1) || ' to ' || ROUND(MAX(rise), 1) AS rise_pts,
       ROUND(MEDIAN(share_same), 1)                    AS median_share_chg_19_22_pct,
       ROUND(MEDIAN(share_next), 1)                    AS median_share_chg_22_23_pct,
       ROUND(100 * AVG((share_next < 0)::INT), 0)      AS pct_lost_share_22_23,
       NULL::DOUBLE AS corr_same_window, NULL::DOUBLE AS corr_next_yr,
       NULL::DOUBLE AS spearman_next_yr, NULL::DOUBLE AS t_next_yr, NULL::BOOLEAN AS check_share_ok
FROM q GROUP BY quarter
UNION ALL
SELECT 'all', COUNT(*),
       ROUND(MIN(rise), 1) || ' to ' || ROUND(MAX(rise), 1),
       ROUND(MEDIAN(share_same), 1), ROUND(MEDIAN(share_next), 1),
       ROUND(100 * AVG((share_next < 0)::INT), 0),
       ROUND(ANY_VALUE(c_same), 3), ROUND(ANY_VALUE(c_next), 3), ROUND(ANY_VALUE(s_next), 3),
       ROUND(ANY_VALUE(c_next) * SQRT((ANY_VALUE(n_next) - 2) / (1 - ANY_VALUE(c_next) ^ 2)), 2),
       ANY_VALUE(chk.ok)
FROM q, stats, chk
ORDER BY complaint_rise_group
