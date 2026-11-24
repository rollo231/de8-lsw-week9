"""Q3 — plain Python Kafka consumer group with manually managed offsets.

Run two instances at the same time (EC2 host, venv):
    ~/venv/bin/python part1-ev/charger_consumer.py --name 0
    ~/venv/bin/python part1-ev/charger_consumer.py --name 1

Offset management
- enable_auto_commit=False : nothing is committed in the background.
- After a record is processed, its NEXT offset (offset + 1) is committed synchronously for that
  partition, so the committed position never runs ahead of the work actually done
  (at-least-once; a crash re-delivers only the record that was being processed).
- Rebalances only happen inside poll(). By then every record returned by the previous poll()
  has been processed AND committed, so the instance that receives a revoked partition starts
  exactly where this one stopped (no double counting across the two instances).
  (kafka-python 3.x runs rebalance callbacks on its IO thread, where a blocking commit() is
  not allowed — the listener therefore only logs.)
- max_poll_records=10 : an instance returns to poll() every 10 records, so a rebalance caused by
  the other instance joining/leaving takes effect quickly instead of after a 500-record batch.
Exit
- When no record arrives for --idle-exit seconds the instance prints its endedBy counts and
  leaves the group. While both instances idle, the group shows 2 members with lag 0.
"""
import argparse
import time
from collections import Counter

from kafka import ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata
from kafka.serializer import DefaultSerializer, JsonSerializer


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


class LogRebalance(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        if revoked:
            log(f"[회수 파티션] {sorted(tp.partition for tp in revoked)}")

    def on_partitions_assigned(self, assigned):
        log(f"[할당 파티션] {sorted(tp.partition for tp in assigned)}")

    def on_partitions_lost(self, lost):
        log(f"[유실 파티션] {sorted(tp.partition for tp in lost)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092,localhost:9093,localhost:9094")
    ap.add_argument("--topic", default="ev-charging-sessions")
    ap.add_argument("--group", default="ev-consumer-lsw")
    ap.add_argument("--name", default="0", help="instance label")
    ap.add_argument("--process-delay", type=float, default=0.05, help="simulated work per record (s)")
    ap.add_argument("--idle-exit", type=float, default=90, help="stop after this many idle seconds")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap.split(","),
        group_id=args.group,
        client_id=f"{args.group}-{args.name}",
        enable_auto_commit=False,          # offsets are committed by this code only
        auto_offset_reset="earliest",      # a new group starts from the beginning of the topic
        max_poll_records=10,
        key_deserializer=DefaultSerializer(),
        value_deserializer=JsonSerializer(),
    )
    ended_by, per_partition = Counter(), Counter()
    consumer.subscribe([args.topic], listener=LogRebalance())
    log(f"===== consumer #{args.name} ===== group_id={args.group} 소비 시작")
    last_record = time.time()
    try:
        while time.time() - last_record < args.idle_exit:
            batches = consumer.poll(timeout_ms=1000)
            for tp, records in batches.items():
                for rec in records:
                    ended_by[rec.value["endedBy"]] += 1           # the "processing"
                    per_partition[tp.partition] += 1
                    time.sleep(args.process_delay)
                    consumer.commit({tp: OffsetAndMetadata(rec.offset + 1, "", -1)})
                last_record = time.time()
    finally:
        consumer.close()   # leaves the group; auto-commit is off, so close() commits nothing

    log(f"===== consumer #{args.name} 결과 =====")
    print(f"처리 건수: {sum(per_partition.values())}건 (partition {dict(sorted(per_partition.items()))})")
    for k, v in sorted(ended_by.items(), key=lambda kv: -kv[1]):
        print(f"  endedBy={k}: {v}")


if __name__ == "__main__":
    main()
