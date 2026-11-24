"""Q4 + Q5 — Spark Structured Streaming on Kafka ev-charging-sessions.

Q4: Bronze (raw events) / Silver (parsed + locationGroup, totalSec, chargingSec, idleSec) to S3 by dt,
    and the cumulative session count per locationGroup printed on every trigger.
Q5: in the same foreachBatch, sessions with idleSec > 5400 (port occupied > 90 min after charging)
    are sent to Slack in real time, and the per-locationGroup anomaly count is written to Gold.

Submit from the EC2 host (driver runs in the spark-master container, executors on spark-worker).
The Slack webhook is never stored in code/compose/files: it is read from SSM Parameter Store with
the EC2 instance role at launch time and passed only as an environment variable.
    docker exec -e SLACK_WEBHOOK_URL="$(aws ssm get-parameter --region ap-northeast-2 \
        --name /de9-lsw/slack-webhook-url --with-decryption --query Parameter.Value --output text)" \
      spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
        --conf spark.log.level=ERROR /opt/de9/part1-ev/charger_stream.py

Two streaming queries share the Kafka source:
  1) foreachBatch  -> Bronze, Silver, Slack alerts, Gold
  2) aggregation   -> groupBy(locationGroup).count() in "complete" mode to the console:
                      the state store keeps the running totals, so each trigger prints the
                      cumulative count for every location group.
maxOffsetsPerTrigger splits the 175 already-produced events into several micro-batches.
S3 access uses only the EC2 instance role (IAMInstanceCredentialsProvider) — no access keys.

Athena (Query Editor, workgroup primary) — Q5 table over the Gold path:
    CREATE DATABASE IF NOT EXISTS de9_lsw;
    CREATE EXTERNAL TABLE IF NOT EXISTS de9_lsw.ev_charging_agg (
        locationGroup string,
        anomaly_count bigint)
    STORED AS PARQUET
    LOCATION 's3://de9-lsw-week9/gold/ev_charging_agg/';
    SELECT locationGroup, anomaly_count
    FROM de9_lsw.ev_charging_agg
    ORDER BY anomaly_count DESC, locationGroup;
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

TOPIC = "ev-charging-sessions"
IDLE_THRESHOLD_SEC = 5400   # 90 minutes

# Explicit schema of the producer's JSON message (Q2)
EVENT_SCHEMA = StructType([
    StructField("stationName", StringType()),
    StructField("portNumber", IntegerType()),
    StructField("portType", StringType()),
    StructField("plugType", StringType()),
    StructField("startTime", StringType()),
    StructField("endTime", StringType()),
    StructField("totalDuration", StringType()),     # hh:mm:ss
    StructField("chargingDuration", StringType()),  # hh:mm:ss
    StructField("energyKwh", DoubleType()),
    StructField("endedBy", StringType()),
])


def post_slack(webhook, text, retries=3):
    """Send one message to a Slack incoming webhook (stdlib only). Honors 429 Retry-After."""
    body = json.dumps({"text": text}).encode("utf-8")
    for attempt in range(retries):
        req = urllib.request.Request(webhook, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                time.sleep(int(e.headers.get("Retry-After", "1")))
                continue
            print(f"[slack] HTTP {e.code}", flush=True)
            return False
        except urllib.error.URLError as e:
            print(f"[slack] {e.reason}", flush=True)
            return False
    return False


def hms_to_sec(c):
    """'h:mm:ss' -> seconds (hours may exceed 24)."""
    p = F.split(c, ":")
    return p[0].cast("int") * 3600 + p[1].cast("int") * 60 + p[2].cast("int")


def to_silver(raw_df):
    """Parse the Kafka value with the explicit schema and add the derived columns."""
    e = F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")
    return (raw_df.select(e, F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
            .select("e.*", "kafka_partition", "kafka_offset")
            .withColumn("locationGroup", F.regexp_replace("stationName", r" #\d+$", ""))
            .withColumn("startTime", F.to_timestamp("startTime", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("endTime", F.to_timestamp("endTime", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("totalSec", hms_to_sec(F.col("totalDuration")))
            .withColumn("chargingSec", hms_to_sec(F.col("chargingDuration")))
            .withColumn("idleSec", F.col("totalSec") - F.col("chargingSec"))
            .withColumn("dt", F.date_format("startTime", "yyyy-MM-dd")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="de9-lsw-week9")
    ap.add_argument("--bootstrap", default="kafka1:19092,kafka2:19092,kafka3:19092")
    ap.add_argument("--max-offsets", type=int, default=70, help="maxOffsetsPerTrigger")
    ap.add_argument("--trigger", default="20 seconds")
    ap.add_argument("--root", default="", help="optional path prefix (e.g. _test/) for trial runs")
    ap.add_argument("--idle-stop", type=int, default=40,
                    help="stop after this many seconds without new input (0 = run forever)")
    args = ap.parse_args()

    base = f"s3a://{args.bucket}/{args.root}".rstrip("/")
    bronze_path = f"{base}/bronze/{TOPIC}"
    silver_path = f"{base}/silver/{TOPIC}"
    gold_path = f"{base}/gold/ev_charging_agg"
    ckpt = f"{base}/checkpoints/{TOPIC}"

    spark = (SparkSession.builder.appName("charger_stream")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
             .config("spark.hadoop.fs.s3a.endpoint.region", "ap-northeast-2")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "false")   # not used by streaming queries
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook:
        print("[slack] SLACK_WEBHOOK_URL not set -> alerts are printed only", flush=True)

    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", TOPIC)
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", args.max_offsets)
              .load())

    # ---- 1) Bronze + Silver (Q4) and anomaly alert + Gold (Q5) via foreachBatch ------------
    def process_batch(batch_df, batch_id):
        batch_df.persist()
        n = batch_df.count()
        bronze = (batch_df.select(
                      F.col("key").cast("string").alias("key"),
                      F.col("value").cast("string").alias("value"),        # original JSON, untouched
                      "topic", "partition", "offset",
                      F.col("timestamp").alias("kafka_timestamp"))
                  .withColumn("dt", F.substring(F.get_json_object("value", "$.startTime"), 1, 10)))
        bronze.write.mode("append").partitionBy("dt").parquet(bronze_path)
        silver = to_silver(batch_df).persist()
        silver.write.mode("append").partitionBy("dt").parquet(silver_path)
        print(f"[bronze/silver] batch {batch_id}: {n} events -> {bronze_path}/ , {silver_path}/", flush=True)

        # Q5 — real-time detection on this micro-batch's Silver rows
        alerts = (silver.filter(F.col("idleSec") > IDLE_THRESHOLD_SEC)
                  .select("stationName", "locationGroup", "startTime", "idleSec")
                  .orderBy("startTime", "stationName").collect())
        sent = 0
        for r in alerts:
            text = (f"[포트 점유 이상] {r.stationName} (설치구역: {r.locationGroup})"
                    f" / 시작 {r.startTime:%Y-%m-%d %H:%M:%S} / idleSec={r.idleSec}")
            if webhook:
                sent += post_slack(webhook, text)
                time.sleep(1.1)              # incoming webhooks allow ~1 message per second
            else:
                print(text, flush=True)
        # Gold — cumulative anomaly count per locationGroup over all Silver so far (overwrite)
        gold = (batch_df.sparkSession.read.parquet(silver_path)
                .filter(F.col("idleSec") > IDLE_THRESHOLD_SEC)
                .groupBy("locationGroup").agg(F.count("*").alias("anomaly_count"))
                .orderBy(F.desc("anomaly_count"), "locationGroup"))
        gold.coalesce(1).write.mode("overwrite").parquet(gold_path)
        print(f"[alert] batch {batch_id}: {len(alerts)} anomalies (idleSec>{IDLE_THRESHOLD_SEC}), "
              f"slack sent {sent} | gold -> {gold_path}/", flush=True)
        silver.unpersist()
        batch_df.unpersist()

    q_store = (source.writeStream.foreachBatch(process_batch)
               .option("checkpointLocation", f"{ckpt}/bronze_silver")
               .trigger(processingTime=args.trigger)
               .queryName("bronze_silver").start())

    # ---- 2) cumulative sessions per locationGroup -> console ------------------------------
    counts = (to_silver(source).groupBy("locationGroup").count()
              .orderBy(F.desc("count"), "locationGroup"))
    q_console = (counts.writeStream.outputMode("complete").format("console")
                 .option("truncate", "false").option("numRows", 50)
                 .option("checkpointLocation", f"{ckpt}/console_agg")
                 .trigger(processingTime=args.trigger)
                 .queryName("location_group_counts").start())

    queries = [q_store, q_console]
    if args.idle_stop <= 0:
        spark.streams.awaitAnyTermination()
        return
    # Stop cleanly once no query has read new input for idle_stop seconds.
    last_input, seen = time.time(), {}
    while all(q.isActive for q in queries):
        time.sleep(2)
        for q in queries:
            p = q.lastProgress
            if p and p["numInputRows"] > 0 and seen.get(q.name) != p["batchId"]:
                seen[q.name] = p["batchId"]
                last_input = time.time()
        if len(seen) == len(queries) and time.time() - last_input >= args.idle_stop:
            break
    for q in queries:
        q.stop()
    print(f"stream stopped (no new input for {args.idle_stop}s)", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
