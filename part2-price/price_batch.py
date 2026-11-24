"""Q7 + Q8 — Spark BATCH job on Kafka product-prices.

Q7: S3 Bronze / Silver + item x month price summary (console).
Q8: per market x item month-over-month change rate (2016-11 -> 2016-12) computed with WINDOW
    functions (lag over market/item ordered by year, month — the two months are never filtered
    apart and recombined), ranked inside each market by |change_rate|, written to S3 Gold;
    the #1 item of every market is printed.

Reads everything currently in the topic once (earliest -> latest offsets), processes it and exits,
so Airflow (Q9) can run it on a schedule. Every run overwrites its outputs, so re-runs are
idempotent (the topic always holds the same 200 records).

Submit (EC2 host; driver in the spark-master container, executors on spark-worker):
    docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        --conf spark.log.level=ERROR /opt/de9/part2-price/price_batch.py

Python 3.8 compatible on purpose: in Q9 the driver runs inside the Airflow container (Python 3.8).
S3 access uses only the EC2 instance role (IAMInstanceCredentialsProvider) — no access keys.
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window

TOPIC = "product-prices"
BASE_MONTH = (2016, 11)     # previous month (price_nov)
TARGET_MONTH = (2016, 12)   # current month  (price_dec)
GOLD_COLUMNS = ["market", "item_name", "price_nov", "price_dec", "change_rate", "change_rank"]

# Explicit schema of the Q6 producer's JSON message
PRICE_SCHEMA = StructType([
    StructField("item_name", StringType()),
    StructField("market", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("price", DoubleType()),
    StructField("currency", StringType()),
    StructField("unit", StringType()),
])


def build_spark():
    spark = (SparkSession.builder.appName("price_batch")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
             .config("spark.hadoop.fs.s3a.endpoint.region", "ap-northeast-2")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_topic(spark, bootstrap):
    """One-shot batch read of everything currently in the topic."""
    return (spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load())


def to_bronze(raw):
    """Original Kafka record, value kept as the untouched JSON string."""
    return raw.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_timestamp"))


def to_silver(bronze):
    """Parse JSON, keep price as a double, drop records whose price is missing or <= 0."""
    return (bronze.select(F.from_json("value", PRICE_SCHEMA).alias("p"),
                          F.col("partition").alias("kafka_partition"),
                          F.col("offset").alias("kafka_offset"))
            .select("p.*", "kafka_partition", "kafka_offset")
            .withColumn("price", F.col("price").cast("double"))
            .filter(F.col("price").isNotNull() & (F.col("price") > 0)))


def item_month_summary(silver):
    # Every (item, month) group holds 10 prices with 2 decimals, so the exact mean ends at the
    # 3rd decimal. Rounding the double average to 3 decimals removes only floating-point noise
    # (e.g. Tomatoes/11 = 20.294999999999998 -> 20.295) without changing the true value.
    return (silver.groupBy("item_name", "month")
            .agg(F.round(F.avg("price"), 3).alias("avg_price"),
                 F.max("price").alias("max_price"),
                 F.min("price").alias("min_price"))
            .orderBy("item_name", "month"))


def price_change_rank(silver):
    """Q8: window functions only.
    1) lag(price) over (market, item ordered by year, month) puts the previous month's price on
       the current month's row;
    2) keep the target month, change_rate = (dec - nov) / nov * 100 rounded to 2 decimals;
    3) rank() over (market ordered by |change_rate| desc) — ties share a rank, direction ignored.
    """
    by_item_time = Window.partitionBy("market", "item_name").orderBy("year", "month")
    with_prev = (silver
                 .withColumn("prev_price", F.lag("price").over(by_item_time))
                 .withColumn("prev_year", F.lag("year").over(by_item_time))
                 .withColumn("prev_month", F.lag("month").over(by_item_time)))
    current = with_prev.filter(
        (F.col("year") == TARGET_MONTH[0]) & (F.col("month") == TARGET_MONTH[1])
        & (F.col("prev_year") == BASE_MONTH[0]) & (F.col("prev_month") == BASE_MONTH[1]))
    changed = (current.select(
                   "market", "item_name",
                   F.col("prev_price").alias("price_nov"),
                   F.col("price").alias("price_dec"))
               .withColumn("change_rate",
                           F.round((F.col("price_dec") - F.col("price_nov")) / F.col("price_nov") * 100, 2)))
    by_market = Window.partitionBy("market").orderBy(F.abs(F.col("change_rate")).desc())
    return changed.withColumn("change_rank", F.rank().over(by_market)).select(*GOLD_COLUMNS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="de9-lsw-week9")
    ap.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    ap.add_argument("--root", default="", help="optional path prefix (e.g. _test/) for trial runs")
    args = ap.parse_args()

    base = "s3a://{}/{}".format(args.bucket, args.root).rstrip("/")
    bronze_path = "{}/bronze/{}".format(base, TOPIC)
    silver_path = "{}/silver/{}".format(base, TOPIC)
    gold_path = "{}/gold/grocery_price_change".format(base)

    spark = build_spark()
    bronze = to_bronze(read_topic(spark, args.bootstrap)).cache()
    bronze.write.mode("overwrite").parquet(bronze_path)
    print("Bronze 적재: {}건 -> {}/".format(bronze.count(), bronze_path), flush=True)

    silver = to_silver(bronze).cache()
    silver.write.mode("overwrite").parquet(silver_path)
    print("Silver 적재: {}건 -> {}/".format(silver.count(), silver_path), flush=True)

    print("=== Q7 품목별 월별 평균/최고/최저가 ===", flush=True)
    item_month_summary(silver).show(50, truncate=False)

    gold = price_change_rank(silver).cache()
    gold.coalesce(1).write.mode("overwrite").parquet(gold_path)
    print("Gold 적재: {}건 -> {}/".format(gold.count(), gold_path), flush=True)

    print("=== Q8 도시별 변동률 1위 ===", flush=True)
    gold.filter(F.col("change_rank") == 1).orderBy("market", "item_name").show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
