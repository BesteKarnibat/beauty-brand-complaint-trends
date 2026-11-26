"""Measure the data traps (the 'I understand my data' evidence).

Findings (Nov 16, 2026):
  - review key (user_id, parent_asin, timestamp) repeats: 495,107 rows (~1.0%)
  - 51.6% of beauty reviews are on a variant asin (color/size) -> group on parent_asin
  - brand: 69.6% details.Brand, 27.7% store only, 2.7% neither (beauty products)
  - price missing for 63.0% of beauty products
  - 10.5% of reviews are under 20 characters
  - 91.1% verified purchase
  - 0 of 1,939,067 products appear in more than one category file

Ran on Dataproc in ~2 minutes.
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("eda_traps").getOrCreate()
B = "gs://adsp32030-amazon-reviews/"
rev = spark.read.parquet(B + "parquet/reviews")
meta = spark.read.parquet(B + "parquet/meta")

print("=== Reviews: is the key unique? variants? verified? ===")
rev.groupBy("category").agg(
    F.count("*").alias("n_reviews"),
    F.countDistinct("user_id", "parent_asin", "timestamp").alias("n_unique_keys"),
    F.round(F.avg((F.col("asin") != F.col("parent_asin")).cast("int")), 4).alias("share_variant_asin"),
    F.round(F.avg(F.col("verified_purchase").cast("int")), 4).alias("share_verified"),
    F.round(F.avg((F.length("text") < 20).cast("int")), 4).alias("share_text_under_20_chars"),
).show(truncate=False)

print("=== Products: where does the brand come from? price missing? ===")
meta.groupBy("category").agg(
    F.count("*").alias("n_products"),
    F.round(F.avg(F.col("brand_detail").isNotNull().cast("int")), 4).alias("share_brand_in_details"),
    F.round(F.avg((F.col("brand_detail").isNull() & F.col("store").isNotNull()).cast("int")), 4).alias("share_store_only"),
    F.round(F.avg(F.col("price").isNull().cast("int")), 4).alias("share_price_missing"),
).show(truncate=False)

print("=== Products listed in more than one category file ===")
multi = meta.groupBy("parent_asin").agg(F.countDistinct("category").alias("n_cat"))
multi.groupBy("n_cat").count().orderBy("n_cat").show()
