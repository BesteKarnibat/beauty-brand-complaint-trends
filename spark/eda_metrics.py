"""Silver -> Gold (in memory) -> Platinum marts + EDA result tables.

Joins every review to its product, builds a simple brand key
(lowercase, accents stripped, non-alphanumerics removed), and computes the
share of 1-2 star reviews per brand x category x year.

Outputs:
  marts/brand_year_metrics/            Parquet, one row per brand x category x year
  results/category_year/               CSV, the category baseline
  results/rating_dist/                 CSV
  results/data_quality/                CSV
  results/top_brands_beauty/           CSV
  results/rising_brands_2019_2022/     CSV

Ran on Dataproc in ~4 minutes.
KNOWN ISSUES (to fix next): duplicates not yet removed (~1%); the mart is
written as ~1,000 small files (add .coalesce before writing).
"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("eda_metrics").getOrCreate()
B = "gs://adsp32030-amazon-reviews/"
R = B + "results/"

def save(d, name):
    d.coalesce(1).write.mode("overwrite").option("header", True).csv(R + name)

rev = spark.read.parquet(B + "parquet/reviews") \
    .select("parent_asin", "rating", "year", "category", "verified_purchase")

meta = (spark.read.parquet(B + "parquet/meta")
        .dropDuplicates(["parent_asin"])
        .withColumn("brand_raw", F.coalesce("brand_detail", "store"))
        .withColumn("brand_key", F.regexp_replace(
            F.translate(F.lower("brand_raw"), "éèêëáàâäíìîïóòôöúùûüñç", "eeeeaaaaiiiioooouuuunc"),
            "[^a-z0-9]", ""))
        .select("parent_asin", "brand_raw", "brand_key", F.lit(1).alias("has_meta")))

df = (rev.join(meta, "parent_asin", "left")
         .withColumn("low_star", (F.col("rating") <= 2).cast("int"))
         .cache())

# EDA 1: reviews and low-star rate per category per year (the baseline)
cy = (df.groupBy("category", "year")
        .agg(F.count("*").alias("n_reviews"),
             F.avg("low_star").alias("low_star_rate"),
             F.avg("rating").alias("avg_rating"))
        .orderBy("category", "year").cache())
save(cy, "category_year")

# EDA 2: rating distribution
save(df.groupBy("category", "rating").count().orderBy("category", "rating"), "rating_dist")

# EDA 3: data quality - how many reviews have product info and a brand
save(df.groupBy("category").agg(
        F.count("*").alias("n_reviews"),
        F.avg(F.coalesce("has_meta", F.lit(0))).alias("share_with_product_info"),
        F.avg(((F.col("brand_key").isNotNull()) & (F.col("brand_key") != "")).cast("int")).alias("share_with_brand")),
     "data_quality")

# Mart: brand x category x year
brand_year = (df.where((F.col("brand_key").isNotNull()) & (F.col("brand_key") != ""))
                .groupBy("category", "brand_key", "year")
                .agg(F.count("*").alias("n_reviews"),
                     F.avg("low_star").alias("low_star_rate"),
                     F.avg("rating").alias("avg_rating"),
                     F.max("brand_raw").alias("brand_example"))
                .cache())
brand_year.write.mode("overwrite").parquet(B + "marts/brand_year_metrics")

beauty = brand_year.where("category = 'Beauty_and_Personal_Care'")
save(beauty.groupBy("brand_key")
           .agg(F.sum("n_reviews").alias("n_reviews"), F.max("brand_example").alias("brand"))
           .orderBy(F.desc("n_reviews")).limit(30), "top_brands_beauty")

# First result: which big beauty brands' low-star rates rose most, 2019 -> 2022
y1 = beauty.where("year = 2019 AND n_reviews >= 1000").select(
        "brand_key", F.col("brand_example").alias("brand"),
        F.col("low_star_rate").alias("rate_2019"), F.col("n_reviews").alias("n_2019"))
y2 = beauty.where("year = 2022 AND n_reviews >= 1000").select(
        "brand_key", F.col("low_star_rate").alias("rate_2022"), F.col("n_reviews").alias("n_2022"))
rising = (y1.join(y2, "brand_key")
            .withColumn("change_pct_points", F.round((F.col("rate_2022") - F.col("rate_2019")) * 100, 2))
            .orderBy(F.desc("change_pct_points")))
save(rising.limit(50), "rising_brands_2019_2022")

print("=== All_Beauty by year (compare with DuckDB) ===")
cy.where("category = 'All_Beauty'").show(40)
print("=== Top 20 rising beauty brands, 2019 -> 2022 ===")
rising.show(20, truncate=False)
