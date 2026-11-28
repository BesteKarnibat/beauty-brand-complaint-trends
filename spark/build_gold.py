"""Silver -> Gold (saved) -> Platinum: dedupe reviews, brand_alias, star schema, marts.

Reads:
  parquet/reviews   Silver reviews (category/year partitions), 50,244,263 rows
  parquet/meta      Silver products, 1,939,067 rows

Writes Gold (new prefix gold/, Silver is left untouched):
  gold/fact_review/    one row per review after dedupe on (user_id, parent_asin, timestamp);
                       no review text (text_len only), partitioned by category/year
  gold/dim_product/    one row per parent_asin, with brand_raw, brand_norm, brand_key, brand_sk
  gold/brand_alias/    one row per raw brand spelling -> canonical brand_key (+ which rule)
  gold/dim_brand/      one row per canonical brand_key
  gold/dim_category/   one row per category file
  gold/dim_date/       one row per review_date

Rewrites Platinum (coalesced, replaces the 1,001-file mart):
  marts/brand_year_metrics/    brand x category x year, now with share_of_category_reviews
  results/category_year/       CSV, category baseline (deduped)
  results/top_brands_beauty/   CSV, top 100 beauty brands after brand_alias
  results/brand_alias_merges/  CSV, every spelling the alias merged, for a hand check
  results/gold_checks/         CSV, the PASS/FAIL checks printed at the end

Dedupe keeps one copy per key: the one with the most helpful votes, then verified,
then higher rating (deterministic, unlike dropDuplicates).
brand_alias only merges spellings into the top 100 beauty brands: an exact manual map,
plus "top brand + known suffix" (L'Oreal Paris -> loreal, Dove Men -> dove).
All other brands keep their old brand_key, so results stay comparable with eda_metrics.py.

Ran on Dataproc (1 master + 2 n2-standard-4, image 2.2) in 8.8-9.1 min (Nov 26-27, 2026).
Result: 49,749,156 reviews after dedupe (495,107 removed, as measured by eda_traps.py);
only 4 duplicate keys had copies with different ratings; 51 spellings merged into
top-100 brands (L'Oreal Professionnel kept separate; Norelco counted as Philips); brand_year_metrics written as 4 files. All 21 checks passed.
"""
import time
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("build_gold").getOrCreate()
B = "gs://adsp32030-amazon-reviews/"
G = B + "gold/"
R = B + "results/"
t0 = time.time()

KEY = ["user_id", "parent_asin", "timestamp"]
BEAUTY = ["All_Beauty", "Beauty_and_Personal_Care"]
KNOWN_ROWS = {"All_Beauty": 701_528, "Beauty_and_Personal_Care": 23_911_390,
              "Health_and_Household": 25_631_345}
KNOWN_DUPS = {"All_Beauty": 7_276, "Beauty_and_Personal_Care": 242_535,
              "Health_and_Household": 245_296}
KNOWN_PRODUCTS = 1_939_067

checks = []
def check(name, ok, detail=""):
    checks.append((name, "PASS" if ok else "FAIL", str(detail)))

def save_csv(d, name):
    d.coalesce(1).write.mode("overwrite").option("header", True).csv(R + name)

def norm(c):
    """Same brand key as eda_metrics.py: lowercase, accents translated, non-alphanumerics removed."""
    k = F.regexp_replace(F.translate(F.lower(c), "éèêëáàâäíìîïóòôöúùûüñç",
                                     "eeeeaaaaiiiioooouuuunc"), "[^a-z0-9]", "")
    return F.when(k != "", k)

# ---------------------------------------------------------------------------
# 1. fact_review: dedupe. Text is dropped before the shuffle (only its length is kept).
# ---------------------------------------------------------------------------
silver = spark.read.parquet(B + "parquet/reviews")
rev = silver.select("category", "year", *KEY, "asin", "rating", "helpful_vote",
                    "verified_purchase", "review_ts", F.length("text").alias("text_len"))

fact = (rev.groupBy("category", "year", *KEY)
           .agg(F.count("*").alias("n_copies"),
                (F.min("rating") != F.max("rating")).alias("copies_disagree_on_rating"),
                F.max(F.struct("helpful_vote", "verified_purchase", "rating", "text_len",
                               "asin", "review_ts")).alias("k"))
           .select("category", "year", *KEY, "k.asin", "k.rating", "k.helpful_vote",
                   "k.verified_purchase", "k.review_ts", "k.text_len",
                   "n_copies", "copies_disagree_on_rating")
           .withColumn("review_date", F.to_date("review_ts"))
           .withColumn("low_star", (F.col("rating") <= 2).cast("int")))

# One file per category/year directory (avoids thousands of small files).
(fact.repartition("category", "year")
     .write.mode("overwrite").partitionBy("category", "year")
     .parquet(G + "fact_review"))
fact = spark.read.parquet(G + "fact_review")   # read back: later steps don't redo the dedupe

before = {r["category"]: r["count"] for r in silver.groupBy("category").count().collect()}
after = {r["category"]: r["n"] for r in fact.groupBy("category").agg(F.count("*").alias("n")).collect()}
for c in KNOWN_ROWS:
    check(f"silver rows {c}", before.get(c) == KNOWN_ROWS[c], before.get(c))
    removed = before.get(c, 0) - after.get(c, 0)
    check(f"dupes removed {c}", removed == KNOWN_DUPS[c], f"{removed} (known {KNOWN_DUPS[c]})")
n_fact = sum(after.values())
n_conflict = fact.where("copies_disagree_on_rating").count()
check("fact_review rows", n_fact == sum(KNOWN_ROWS.values()) - sum(KNOWN_DUPS.values()), n_fact)
check("dupe keys whose copies have different ratings (info)", True, n_conflict)

# ---------------------------------------------------------------------------
# 2. Products and brand_norm (the old brand_key)
# ---------------------------------------------------------------------------
meta = spark.read.parquet(B + "parquet/meta")
prod = (meta.dropDuplicates(["parent_asin"])
            .withColumn("brand_raw", F.coalesce("brand_detail", "store"))
            .withColumn("brand_norm", norm(F.col("brand_raw")))
            .cache())
n_meta, n_prod = meta.count(), prod.count()
check("silver products", n_meta == KNOWN_PRODUCTS, n_meta)
check("dim_product rows after dedupe on parent_asin (info)", True, n_prod)

# Pre-aggregate reviews to product x year before joining: 49.7M rows -> a few million.
py = (fact.groupBy("category", "parent_asin", "year")
          .agg(F.count("*").alias("n"),
               F.sum("low_star").alias("n_low"),
               F.sum("rating").alias("sum_rating"),
               F.sum(F.col("verified_purchase").cast("int")).alias("n_verified"),
               F.sum(F.when(F.col("verified_purchase"), F.col("low_star")).otherwise(0)).alias("n_low_verified")))
pyb = (py.join(prod.select("parent_asin", "brand_raw", "brand_norm",
                           F.lit(1).alias("has_product")), "parent_asin", "left")
         .cache())

tot = pyb.agg(F.sum("n").alias("n"),
              F.sum(F.when(F.col("has_product") == 1, F.col("n"))).alias("n_prod")).first()
check("no fan-out in product join", tot["n"] == n_fact, tot["n"])
check("share of reviews with product info", tot["n_prod"] == n_fact, round(tot["n_prod"] / n_fact, 5))

# ---------------------------------------------------------------------------
# 3. brand_alias for the top 100 beauty brands (small; built in Python on the driver)
# ---------------------------------------------------------------------------
top100 = [r["brand_norm"] for r in
          pyb.where(F.col("category").isin(BEAUTY) & F.col("brand_norm").isNotNull())
             .groupBy("brand_norm").agg(F.sum("n").alias("n"))
             .orderBy(F.desc("n"), "brand_norm").limit(100).collect()]
all_keys = {r["brand_norm"] for r in
            prod.where(F.col("brand_norm").isNotNull()).select("brand_norm").distinct().collect()}

# Exact merges checked by hand. Extend after reading results/brand_alias_merges.
MANUAL = {
    "lorealparis": "loreal",
    # Salon line, kept as its own brand (decided Nov 26). Listed here so the suffix
    # rule does not strip "professionnel" and fold it into loreal.
    "lorealprofessionnel": "lorealprofessionnel",
    "lorealprofessionnelparis": "lorealprofessionnel",
}
# Hand-checked "starts with" merges, for product lines sold under many model names.
MANUAL_PREFIX = {
    # Norelco is Philips' shaver line (decided Nov 27): Philips Norelco, Norelco,
    # Norelco SmartTouch-XL HQ9100 ... all count as Philips.
    "philipsnorelco": "philips",
    "norelco": "philips",
}

def manual(k):
    """Hand-checked target for brand_norm k, or None."""
    if k in MANUAL:
        return MANUAL[k]
    return next((v for p, v in MANUAL_PREFIX.items() if k.startswith(p)), None)
# Endings that do not change the brand. Root must be >= 3 characters.
SUFFIXES = sorted(["paris", "newyork", "usa", "men", "mens", "formen", "professional",
                   "professionnel", "pro", "beauty", "cosmetics", "cosmetic", "makeup",
                   "skincare", "haircare", "official", "officialstore", "store", "shop",
                   "inc", "llc", "ltd"], key=len, reverse=True)

def bases(k):
    """Successive roots of k after peeling known suffixes: dovemen -> [dove]."""
    out, cur = [], k
    while True:
        s = next((s for s in SUFFIXES if cur.endswith(s) and len(cur) - len(s) >= 3), None)
        if s is None:
            return out
        cur = cur[: -len(s)]
        out.append((cur, s))

# Each top brand's canonical root: the shortest suffix-peeled form that is itself a brand.
root_of = {}
for t in top100:
    if manual(t):
        root_of[t] = (manual(t), "manual")
    else:
        hits = [(b, s) for b, s in bases(t) if b in all_keys]
        root_of[t] = (hits[-1][0], "top100_suffix:" + hits[-1][1]) if hits else (t, "top100")
roots = {r for r, _ in root_of.values()}

alias_rows = []
for k in all_keys:
    if manual(k):
        c, rule = manual(k), "manual"
    elif k in root_of:
        c, rule = root_of[k]
    elif k in roots:
        c, rule = k, "top100_root"
    else:
        hits = [(b, s) for b, s in bases(k) if b in roots]
        c, rule = (hits[-1][0], "suffix:" + hits[-1][1]) if hits else (k, "self")
    alias_rows.append((k, c, rule))
alias = spark.createDataFrame(alias_rows, "brand_norm string, brand_key string, alias_rule string")
is_top = spark.createDataFrame([(r,) for r in roots], "brand_key string").withColumn("is_top100", F.lit(True))

# brand_alias as in the data model: one row per raw spelling
brand_alias = (prod.where(F.col("brand_norm").isNotNull())
                   .groupBy("brand_raw", "brand_norm").agg(F.count("*").alias("n_products"))
                   .join(F.broadcast(alias), "brand_norm")
                   .withColumn("brand_sk", F.xxhash64("brand_key")))
brand_alias.coalesce(1).write.mode("overwrite").parquet(G + "brand_alias")

# ---------------------------------------------------------------------------
# 4. dim_product, dim_brand, dim_category, dim_date
# ---------------------------------------------------------------------------
dim_product = (prod.join(F.broadcast(alias.select("brand_norm", "brand_key")), "brand_norm", "left")
                   .withColumn("brand_sk", F.xxhash64("brand_key"))
                   .select("parent_asin", "category", "main_category", "product_title", "price",
                           "average_rating", "rating_number", "brand_raw", "brand_norm",
                           "brand_key", "brand_sk"))
dim_product.coalesce(8).write.mode("overwrite").parquet(G + "dim_product")

pyk = pyb.join(F.broadcast(alias.select("brand_norm", "brand_key")), "brand_norm", "left").cache()

# Display name = the raw spelling with the most reviews
w = Window.partitionBy("brand_key").orderBy(F.desc("n"), "brand_raw")
brand_name = (pyk.where(F.col("brand_key").isNotNull())
                 .groupBy("brand_key", "brand_raw").agg(F.sum("n").alias("n"))
                 .withColumn("rk", F.row_number().over(w)).where("rk = 1")
                 .select("brand_key", F.col("brand_raw").alias("brand_name")))
brand_reviews = (pyk.where(F.col("brand_key").isNotNull()).groupBy("brand_key")
                    .agg(F.sum("n").alias("n_reviews"),
                         F.sum(F.when(F.col("category").isin(BEAUTY), F.col("n")).otherwise(0)).alias("n_reviews_beauty")))
brand_products = (dim_product.where(F.col("brand_key").isNotNull()).groupBy("brand_key")
                     .agg(F.count("*").alias("n_products"),
                          F.countDistinct("brand_norm").alias("n_spellings"),
                          F.count("price").alias("n_priced_products"),
                          F.percentile_approx("price", 0.5).alias("median_price")))
dim_brand = (brand_products.join(brand_reviews, "brand_key", "left")
                           .join(brand_name, "brand_key", "left")
                           .join(is_top, "brand_key", "left")
                           .withColumn("is_top100", F.coalesce("is_top100", F.lit(False)))
                           .withColumn("brand_sk", F.xxhash64("brand_key"))
                           .cache())
dim_brand.coalesce(4).write.mode("overwrite").parquet(G + "dim_brand")

cy = (pyb.groupBy("category", "year")
         .agg(F.sum("n").alias("n_reviews"),
              (F.sum("n_low") / F.sum("n")).alias("low_star_rate"),
              (F.sum("sum_rating") / F.sum("n")).alias("avg_rating"),
              (F.sum("n_low_verified") / F.sum("n_verified")).alias("low_star_rate_verified"))
         .orderBy("category", "year").cache())
save_csv(cy, "category_year")

dim_category = (cy.groupBy("category").agg(F.sum("n_reviews").alias("n_reviews"))
                  .join(prod.groupBy("category").agg(F.count("*").alias("n_products")), "category")
                  .withColumn("category_group", F.when(F.col("category").isin(BEAUTY), "beauty")
                                                 .otherwise("baseline")))
dim_category.coalesce(1).write.mode("overwrite").parquet(G + "dim_category")

dim_date = (fact.select("review_date").where(F.col("review_date").isNotNull()).distinct()
                .withColumn("year", F.year("review_date"))
                .withColumn("quarter", F.quarter("review_date"))
                .withColumn("month", F.month("review_date"))
                .withColumn("day_of_week", F.dayofweek("review_date"))
                .withColumn("is_partial_year", F.col("year") == 2023))   # data ends Sep 2023
dim_date.coalesce(1).write.mode("overwrite").parquet(G + "dim_date")

# ---------------------------------------------------------------------------
# 5. Platinum: brand x category x year, with share of category reviews
# ---------------------------------------------------------------------------
brand_year = (pyk.where(F.col("brand_key").isNotNull())
                 .groupBy("category", "brand_key", "year")
                 .agg(F.sum("n").alias("n_reviews"),
                      F.count("*").alias("n_products_reviewed"),
                      F.sum("n_low").alias("n_low_star"),
                      (F.sum("n_low") / F.sum("n")).alias("low_star_rate"),
                      (F.sum("sum_rating") / F.sum("n")).alias("avg_rating"),
                      (F.sum("n_low_verified") / F.sum("n_verified")).alias("low_star_rate_verified"))
                 .join(dim_brand.select("brand_key", F.col("brand_name").alias("brand_example"),
                                        "is_top100"), "brand_key", "left")
                 .join(cy.select("category", "year", F.col("n_reviews").alias("cat_n_reviews"),
                                 F.col("low_star_rate").alias("cat_low_star_rate")),
                       ["category", "year"])
                 # Denominator = every review in the category-year, including the ~2% with no brand
                 .withColumn("share_of_category_reviews", F.col("n_reviews") / F.col("cat_n_reviews"))
                 .withColumn("low_star_vs_category_pts",
                             100 * (F.col("low_star_rate") - F.col("cat_low_star_rate")))
                 .cache())
brand_year.coalesce(4).write.mode("overwrite").parquet(B + "marts/brand_year_metrics")

top = (dim_brand.orderBy(F.desc("n_reviews_beauty")).limit(100)
                .select("brand_key", "brand_name", "n_reviews_beauty", "n_reviews",
                        "n_products", "n_spellings", "median_price", "is_top100"))
save_csv(top, "top_brands_beauty")

merges = (pyk.join(F.broadcast(alias.where("brand_norm != brand_key")), ["brand_norm", "brand_key"])
             .groupBy("brand_key", "brand_norm", "alias_rule")
             .agg(F.sum("n").alias("n_reviews"), F.max("brand_raw").alias("brand_raw_example"))
             .orderBy("brand_key", F.desc("n_reviews")))
save_csv(merges, "brand_alias_merges")

# ---------------------------------------------------------------------------
# 6. Checks
# ---------------------------------------------------------------------------
# Category baseline two ways: from the pre-aggregate vs straight from fact_review.
cy_direct = {(r["category"], r["year"]): (r["n"], r["rate"]) for r in
             fact.groupBy("category", "year")
                 .agg(F.count("*").alias("n"), F.avg("low_star").alias("rate")).collect()}
cy_rows = {(r["category"], r["year"]): (r["n_reviews"], r["low_star_rate"]) for r in cy.collect()}
mism = [k for k in cy_direct if cy_direct[k][0] != cy_rows.get(k, (None,))[0]
        or abs(cy_direct[k][1] - cy_rows[k][1]) > 1e-12]
check("category_year: pre-aggregate = direct from fact", not mism, mism[:5])

bp = cy_rows[("Beauty_and_Personal_Care", 2019)][1], cy_rows[("Beauty_and_Personal_Care", 2022)][1]
check("Beauty low-star 2019 near 0.15228 (pre-dedupe)", abs(bp[0] - 0.15228) < 0.005, round(bp[0], 5))
check("Beauty low-star 2022 near 0.21277 (pre-dedupe)", abs(bp[1] - 0.21277) < 0.005, round(bp[1], 5))

tec = {r["year"]: r["low_star_rate"] for r in
       brand_year.where("category = 'Beauty_and_Personal_Care' AND brand_key = 'tecunite' "
                        "AND year IN (2019, 2022)").collect()}
check("TecUnite 2019 near 17.3%", abs(tec.get(2019, -1) - 0.173) < 0.01, tec.get(2019))
check("TecUnite 2022 near 63.4%", abs(tec.get(2022, -1) - 0.634) < 0.01, tec.get(2022))

# Shares in a category-year add up to the branded share (<= 1, ~0.98 for beauty)
ss = brand_year.groupBy("category", "year").agg(F.sum("share_of_category_reviews").alias("s"))
smax = ss.agg(F.max("s")).first()[0]
check("sum of brand shares per category-year <= 1", smax <= 1 + 1e-9, round(smax, 6))

# share_of_category_reviews a second way: Spark SQL straight on the saved Gold tables
# (fact_review JOIN dim_product), not through the product-year pre-aggregate.
mart_share = {r["brand_key"]: r["share_of_category_reviews"] for r in
              brand_year.where("category = 'Beauty_and_Personal_Care' AND year = 2022")
                        .orderBy(F.desc("n_reviews")).limit(5).collect()}
spark.read.parquet(G + "fact_review").createOrReplaceTempView("fact_review")
spark.read.parquet(G + "dim_product").createOrReplaceTempView("dim_product")
keys_sql = ", ".join(f"'{k}'" for k in mart_share)   # brand_key is [a-z0-9] only
sql_share = {r["brand_key"]: r["share"] for r in spark.sql(f"""
    SELECT p.brand_key, COUNT(*) / MAX(t.n) AS share
    FROM fact_review f
    JOIN dim_product p ON f.parent_asin = p.parent_asin
    CROSS JOIN (SELECT COUNT(*) AS n FROM fact_review
                WHERE category = 'Beauty_and_Personal_Care' AND year = 2022) t
    WHERE f.category = 'Beauty_and_Personal_Care' AND f.year = 2022
      AND p.brand_key IN ({keys_sql})
    GROUP BY p.brand_key""").collect()}
bad = {k: (v, sql_share.get(k)) for k, v in mart_share.items()
       if sql_share.get(k) is None or abs(v - sql_share[k]) > 1e-12}
check("share_of_category: mart = Spark SQL on Gold (top 5 brands, Beauty 2022)", not bad,
      bad or {k: round(v, 6) for k, v in mart_share.items()})

n_files = len(spark.read.parquet(B + "marts/brand_year_metrics").inputFiles())
check("brand_year_metrics files <= 4", n_files <= 4, n_files)
check("brand_norm spellings merged into a top-100 brand (info)", True,
      sum(1 for k, c, _ in alias_rows if k != c))

# ---------------------------------------------------------------------------
print("=== Rows removed by dedupe, per category ===")
for c in KNOWN_ROWS:
    print(f"  {c:28s} {before[c]:>12,} -> {after[c]:>12,}")
print("=== Beauty_and_Personal_Care, 2019 vs 2022 ===")
cy.where("category = 'Beauty_and_Personal_Care' AND year IN (2019, 2022)").show()
print("=== Top 20 beauty brands after brand_alias ===")
top.show(20, truncate=False)
print("=== Spellings merged by brand_alias (first 40) ===")
merges.show(40, truncate=False)
print("=== Top 15 by share of category reviews, Beauty 2022 ===")
(brand_year.where("category = 'Beauty_and_Personal_Care' AND year = 2022")
           .orderBy(F.desc("share_of_category_reviews"))
           .select("brand_example", "n_reviews", "share_of_category_reviews",
                   "low_star_rate", "low_star_vs_category_pts").show(15, truncate=False))
print("=== CHECKS ===")
for name, status, detail in checks:
    print(f"  [{status}] {name}: {detail}")
save_csv(spark.createDataFrame(checks, "check string, status string, detail string"), "gold_checks")
print(f"Elapsed: {(time.time() - t0) / 60:.1f} min")
