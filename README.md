https://github.com/rollo231/de8-lsw-week9

# 데엔 8기 9주차 — 이상원 (lsw)

Kafka (3 brokers on one EC2) real-time streaming + S3 Bronze/Silver/Gold batch pipeline.
Everything (Kafka · kafka-ui · Spark standalone · Airflow) runs on **one EC2 t3.large (Ubuntu 22.04)**
from `part1-ev/docker-compose.yaml`. AWS access (S3 · Athena · Glue · SSM) uses only the EC2 IAM role
**`de9-ec2-lsw`** — no access keys in code, compose, env files or commits.

| Identifier | Value |
|---|---|
| Kafka consumer group | `ev-consumer-lsw` |
| Athena database | `de9_lsw` |
| Airflow DAG ID | `price_pipeline_lsw` |
| EC2 IAM role | `de9-ec2-lsw` |
| S3 bucket | `de9-lsw-week9` |

## Layout

```
part1-ev/        Part 1 — EV charging sessions (Q1–Q5)
  docker-compose.yaml   Kafka x3 (KRaft) · kafka-ui v0.7.2 · Spark 3.5.3 master/worker · Airflow 2.10.5
  requirements.txt      kafka-python==3.0.13 (host-side producer / consumer / monitor)
  charger_producer.py   Q2  CSV replay -> ev-charging-sessions (acks=all, idempotent, key=stationName)
  charger_consumer.py   Q3  consumer group, manual per-record commit, rebalance logging
  charger_stream.py     Q4  Structured Streaming -> bronze/silver + cumulative counts
                        Q5  idleSec > 5400 -> Slack, gold ev_charging_agg, Athena DDL
part2-price/     Part 2 — grocery prices (Q6–Q10)
  airflow/Dockerfile    Airflow 2.10.5 (Python 3.8 = Spark cluster) + Java 11 + Spark 3.5.3
  price_producer.py     Q6  custom RegionPartitioner (market -> partition by region)
  price_batch.py        Q7  batch bronze/silver + item x month summary
                        Q8  window-function change rate / rank -> gold grocery_price_change
  dags/price_pipeline_dag.py  Q9  SparkSubmitOperator -> Athena register -> Athena top1 (XCom id)
  kafka_lag_monitor.py  Q10 per-partition current / log-end offset, lag and total lag
data/            provided CSVs (unmodified)
```

## Run (EC2 host, repo cloned at `~/de9`)

```bash
cd ~/de9/part1-ev && docker compose up -d --build            # Q1
python3 -m venv ~/venv && ~/venv/bin/pip install -r requirements.txt

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 \
  --create --topic ev-charging-sessions --partitions 3 --replication-factor 2
cd ~/de9
~/venv/bin/python part1-ev/charger_producer.py                                    # Q2 (once)
~/venv/bin/python part1-ev/charger_consumer.py --name 0 &                         # Q3
~/venv/bin/python part1-ev/charger_consumer.py --name 1 & wait
docker exec -e SLACK_WEBHOOK_URL="$(aws ssm get-parameter --region ap-northeast-2 \
    --name /de9-lsw/slack-webhook-url --with-decryption --query Parameter.Value --output text)" \
  spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=ERROR /opt/de9/part1-ev/charger_stream.py                # Q4 + Q5

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092 \
  --create --topic product-prices --partitions 3 --replication-factor 2
~/venv/bin/python part2-price/price_producer.py                                   # Q6 (once)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=ERROR /opt/de9/part2-price/price_batch.py                # Q7 + Q8
# Q9: unpause price_pipeline_lsw in the Airflow UI (:8090); runs every 10 minutes
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:19092 \
  --group ev-consumer-lsw --topic ev-charging-sessions --reset-offsets --to-earliest --execute
~/venv/bin/python part2-price/kafka_lag_monitor.py ev-consumer-lsw               # Q10
```

The Slack webhook lives only in SSM Parameter Store (SecureString) and is read with the instance
role at launch time. Athena queries use workgroup `primary` (results in `s3://de9-lsw-week9/athena-results/`).
