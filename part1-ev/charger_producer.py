"""Q2 — EV charging session producer (replay of the provided CSV as a live stream).

Runs on the EC2 host (venv with requirements.txt) against the 3-broker Kafka cluster:
    ~/venv/bin/python part1-ev/charger_producer.py

Delivery guarantees
- acks="all"                 : a write is acknowledged only after every in-sync replica has it,
                               so losing one broker cannot lose an acknowledged message.
- enable_idempotence=True    : the broker de-duplicates producer retries (producer id + sequence
                               number), so a retry after a broker failure cannot store a duplicate.
- retries / delivery_timeout : keep retrying while leadership moves to the surviving replica.
- max_in_flight <= 5         : required for idempotence and keeps per-partition ordering.
Partitioning
- key = stationName          : the default partitioner hashes the key (murmur2), so every session
                               of the same charger always lands in the same partition.
"""
import argparse
import csv
import random
import time
from collections import Counter
from pathlib import Path

from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "data" / "charger_sessions.csv"
INT_FIELDS = {"portNumber"}
FLOAT_FIELDS = {"energyKwh"}
FIELDS = ["stationName", "portNumber", "portType", "plugType", "startTime", "endTime",
          "totalDuration", "chargingDuration", "energyKwh", "endedBy"]


def to_message(row):
    msg = {}
    for f in FIELDS:
        v = row[f]
        msg[f] = int(v) if f in INT_FIELDS else float(v) if f in FLOAT_FIELDS else v
    return msg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092,localhost:9093,localhost:9094")
    ap.add_argument("--topic", default="ev-charging-sessions")
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    args = ap.parse_args()

    with open(args.csv, newline="", encoding="utf-8") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r["startTime"])  # stable sort by start time

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        acks="all",
        enable_idempotence=True,
        retries=2147483647,
        max_in_flight_requests_per_connection=5,
        delivery_timeout_ms=120000,
        request_timeout_ms=30000,
        key_serializer=DefaultSerializer(),    # str  -> UTF-8 bytes
        value_serializer=JsonSerializer(),     # dict -> JSON (UTF-8)
    )
    futures = []
    try:
        print(f"replay start: {len(rows)} rows -> topic={args.topic}")
        for row in rows:
            msg = to_message(row)
            futures.append(producer.send(args.topic, key=msg["stationName"], value=msg))
            time.sleep(random.uniform(0.05, 0.2))
    finally:
        producer.flush()   # wait until every buffered record is acknowledged (or failed)
        producer.close()

    per_partition, failed = Counter(), 0
    for fut in futures:
        try:
            per_partition[fut.get(timeout=0).partition] += 1
        except Exception as e:  # surfaced so a lost record can never go unnoticed
            failed += 1
            print(f"send failed: {e!r}")
    sent = sum(per_partition.values())
    print(f"전송 완료: {sent}건 -> topic={args.topic}"
          f" (partition {dict(sorted(per_partition.items()))}, failed={failed})")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
