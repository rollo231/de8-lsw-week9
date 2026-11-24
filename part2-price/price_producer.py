"""Q6 — Grocery price producer with a region-based custom partitioner (kafka-python).

Runs on the EC2 host (venv with part1-ev/requirements.txt):
    ~/venv/bin/python part2-price/price_producer.py

Partition choice is made ONLY by RegionPartitioner, which is plugged into the producer through
the `partitioner` config. send() is never given a partition number.
    partition 0  North          : Delhi, Jaipur, Lucknow, Kanpur
    partition 1  West / Central : Mumbai, Ahmedabad, Bhopal
    partition 2  South / East   : Hyderabad, Chennai, Patna
    any other city              : default rule (murmur2 hash of the key, same as the Java client)
kafka-python 3.x interface: subclass kafka.partitioner.Partitioner and implement
partition(topic, key, serialized_key, value, serialized_value, cluster).
"""
import argparse
import csv
from collections import Counter
from pathlib import Path

from kafka import KafkaProducer
from kafka.partitioner import DefaultPartitioner, Partitioner
from kafka.serializer import DefaultSerializer, JsonSerializer

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "india_grocery_prices.csv"
FIELDS = ["item_name", "market", "year", "month", "price", "currency", "unit"]

REGION_PARTITION = {
    "Delhi": 0, "Jaipur": 0, "Lucknow": 0, "Kanpur": 0,      # North
    "Mumbai": 1, "Ahmedabad": 1, "Bhopal": 1,                # West / Central
    "Hyderabad": 2, "Chennai": 2, "Patna": 2,                # South / East
}


class RegionPartitioner(Partitioner):
    """market (the record key) -> fixed partition by region; unknown cities -> default partitioner."""

    def __init__(self, region_partition=REGION_PARTITION):
        self.region_partition = region_partition
        self.default = DefaultPartitioner()

    def partition(self, topic, key, serialized_key, value, serialized_value, cluster):
        target = self.region_partition.get(key)
        partitions = cluster.partitions_for_topic(topic) or set()
        if target is not None and target in partitions:
            return target
        return self.default.partition(topic, key, serialized_key, value, serialized_value, cluster)


def to_message(row):
    return {
        "item_name": row["item_name"],
        "market": row["market"],
        "year": int(row["year"]),
        "month": int(row["month"]),
        "price": float(row["price"]),
        "currency": row["currency"],
        "unit": row["unit"],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092,localhost:9093,localhost:9094")
    ap.add_argument("--topic", default="product-prices")
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    args = ap.parse_args()

    with open(args.csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        acks="all",
        enable_idempotence=True,
        partitioner=RegionPartitioner(),       # <- custom partitioner decides the partition
        key_serializer=DefaultSerializer(),    # market (str) -> UTF-8
        value_serializer=JsonSerializer(),     # dict -> JSON
    )
    futures = []
    try:
        for row in rows:
            msg = to_message(row)
            futures.append((msg["market"], producer.send(args.topic, key=msg["market"], value=msg)))
    finally:
        producer.flush()   # every record acknowledged (or failed) before we report
        producer.close()

    per_partition, per_city, failed = Counter(), {}, 0
    for city, fut in futures:
        try:
            p = fut.get(timeout=0).partition
        except Exception as e:
            failed += 1
            print(f"send failed: {e!r}")
            continue
        per_partition[p] += 1
        per_city.setdefault(p, set()).add(city)
    print(f"전송 완료: {sum(per_partition.values())}건 -> topic={args.topic} (failed={failed})")
    for p in sorted(per_partition):
        print(f"  partition {p}: {per_partition[p]}건  {sorted(per_city[p])}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
