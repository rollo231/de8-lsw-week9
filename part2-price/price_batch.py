"""Q7 — Spark BATCH job: Kafka product-prices -> S3 Bronze / Silver + item x month price summary.

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

TOPIC = "product-prices"

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="de9-lsw-week9")
    ap.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    ap.add_argument("--root", default="", help="optional path prefix (e.g. _test/) for trial runs")
    args = ap.parse_args()

    base = "s3a://{}/{}".format(args.bucket, args.root).rstrip("/")
    bronze_path = "{}/bronze/{}".format(base, TOPIC)
    silver_path = "{}/silver/{}".format(base, TOPIC)

    spark = build_spark()
    bronze = to_bronze(read_topic(spark, args.bootstrap)).cache()
    bronze.write.mode("overwrite").parquet(bronze_path)
    print("Bronze 적재: {}건 -> {}/".format(bronze.count(), bronze_path), flush=True)

    silver = to_silver(bronze).cache()
    silver.write.mode("overwrite").parquet(silver_path)
    print("Silver 적재: {}건 -> {}/".format(silver.count(), silver_path), flush=True)

    print("=== Q7 품목별 월별 평균/최고/최저가 ===", flush=True)
    item_month_summary(silver).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
