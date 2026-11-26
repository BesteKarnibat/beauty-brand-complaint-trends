"""Bronze -> Silver: raw JSONL in GCS -> typed Parquet.

Reads the 6 raw JSONL files (3 review files, 3 product-metadata files),
gives reviews an explicit schema, converts the millisecond timestamp to a
real timestamp, and writes Parquet partitioned by category and year.
Metadata keeps only the fields the project needs ('details' is messy).

Ran on Dataproc (1 master + 2 n2-standard-4 workers, image 2.2) in ~5 minutes.
Output: 50,244,263 reviews and 1,939,067 products.
"""
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("reviews_to_parquet").getOrCreate()
B = "gs://adsp32030-amazon-reviews/"

# --- Reviews: explicit schema, so Spark doesn't scan 21 GB just to guess types ---
review_schema = T.StructType([
    T.StructField("rating", T.DoubleType()),
    T.StructField("title", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("asin", T.StringType()),
    T.StructField("parent_asin", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("timestamp", T.LongType()),        # milliseconds since epoch
    T.StructField("helpful_vote", T.LongType()),
    T.StructField("verified_purchase", T.BooleanType()),
])

rev = (spark.read.schema(review_schema).json(B + "raw/reviews/*.jsonl")
       .withColumn("category", F.regexp_extract(F.input_file_name(), r"reviews/([^/]+)\.jsonl", 1))
       .withColumn("review_ts", (F.col("timestamp") / 1000).cast("timestamp"))
       .withColumn("year", F.year("review_ts")))

(rev.repartition("category", "year")
    .write.mode("overwrite").partitionBy("category", "year")
    .parquet(B + "parquet/reviews"))

# --- Metadata: pull only the fields we need with get_json_object ---
j = lambda path: F.get_json_object("value", path)
meta = (spark.read.text(B + "raw/meta/*.jsonl")
        .withColumn("category", F.regexp_extract(F.input_file_name(), r"meta_([^/]+)\.jsonl", 1))
        .select(
            j("$.parent_asin").alias("parent_asin"),
            j("$.main_category").alias("main_category"),
            j("$.title").alias("product_title"),
            j("$.store").alias("store"),
            j("$.details.Brand").alias("brand_detail"),
            j("$.price").cast("double").alias("price"),            # "None" -> null
            j("$.average_rating").cast("double").alias("average_rating"),
            j("$.rating_number").cast("long").alias("rating_number"),
            j("$.categories").alias("categories_json"),
            "category"))

meta.write.mode("overwrite").parquet(B + "parquet/meta")

# --- Check: row counts per category ---
spark.read.parquet(B + "parquet/reviews").groupBy("category").count().show()
spark.read.parquet(B + "parquet/meta").groupBy("category").count().show()
