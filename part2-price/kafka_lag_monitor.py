"""Q10 — Consumer group lag monitor (kafka-python).

    ~/venv/bin/python part2-price/kafka_lag_monitor.py ev-consumer-lsw

For every partition of every topic the group has committed offsets for, prints
    topic, partition, current-offset (group's committed offset), log-end-offset, lag
and the total lag. Read-only: the end offsets are fetched by a consumer WITHOUT a group id,
so running this never commits anything or joins / rebalances the monitored group.
"""
import argparse

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("group", help="consumer group id, e.g. ev-consumer-lsw")
    ap.add_argument("--bootstrap", default="localhost:9092,localhost:9093,localhost:9094")
    args = ap.parse_args()
    servers = args.bootstrap.split(",")

    admin = KafkaAdminClient(bootstrap_servers=servers)
    try:
        committed = admin.list_group_offsets(args.group).get(args.group, {})   # {TopicPartition: OffsetAndMetadata}
    finally:
        admin.close()
    if not committed:
        print("group = {}: no committed offsets".format(args.group))
        return

    reader = KafkaConsumer(bootstrap_servers=servers, enable_auto_commit=False)   # no group_id
    try:
        topics = sorted({tp.topic for tp in committed})
        partitions = [TopicPartition(t, p) for t in topics for p in sorted(reader.partitions_for_topic(t) or [])]
        log_end = reader.end_offsets(partitions)
    finally:
        reader.close()

    print("group = {}".format(args.group))
    header = "{:<24}{:>10}{:>16}{:>16}{:>8}".format("topic", "partition", "current-offset", "log-end-offset", "lag")
    print(header)
    print("-" * len(header))
    total = 0
    for tp in partitions:
        om = committed.get(tp)
        current = om.offset if om is not None and om.offset >= 0 else None
        end = log_end[tp]
        lag = end - (current if current is not None else 0)   # no commit yet -> whole partition is lag
        total += lag
        print("{:<24}{:>10}{:>16}{:>16}{:>8}".format(
            tp.topic, tp.partition, "-" if current is None else current, end, lag))
    print("-" * len(header))
    print("{:<66}{:>8}".format("TOTAL LAG", total))


if __name__ == "__main__":
    main()
