https://github.com/rollo231/de8-lsw-week9

# 데엔 8기 9주차 — 이상원 (lsw)

- `part1-ev/` — Part 1: EV charging sessions → Kafka (3 brokers on EC2) → Spark Structured Streaming → S3 bronze/silver/gold, Slack alert, Athena
- `part2-price/` — Part 2: grocery prices → Kafka (custom partitioner) → Spark batch → S3 bronze/silver/gold, Airflow DAG, Athena
- `data/` — provided CSVs (unmodified)

Runs on one EC2 instance (t3.large, Ubuntu 22.04) with IAM role `de9-ec2-lsw`; no AWS access keys are stored anywhere.
