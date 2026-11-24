"""Q9 — price_pipeline_lsw: scheduled Spark batch (Q7+Q8) + Athena registration/query.

Runs every 10 minutes on the EC2 Airflow (docker compose service "airflow"):
  1) spark_price_batch    SparkSubmitOperator -> price_batch.py on spark://spark-master:7077
                          (bronze / silver / gold grocery_price_change refreshed in S3)
  2) register_gold_table  AthenaOperator: CREATE EXTERNAL TABLE de9_lsw.grocery_price_change
                          over the Q8 Gold path
  3) query_top1           AthenaOperator: SELECT each market's #1 item; the operator returns the
                          Athena query execution id, which Airflow stores as the XCom return_value
max_active_runs=1 keeps runs from overlapping. AWS access (Athena, S3, Glue) uses only the EC2
instance role: aws_conn_id=None -> boto3 default credential chain -> instance metadata.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = "price_pipeline_lsw"
BUCKET = "de9-lsw-week9"
ROOT = ""                                   # optional S3 path prefix (trial runs only)
DATABASE = "de9_lsw"
TABLE = "grocery_price_change"
GOLD_LOCATION = "s3://{}/{}gold/grocery_price_change/".format(BUCKET, ROOT)
ATHENA_OUTPUT = "s3://{}/athena-results/".format(BUCKET)
REGION = "ap-northeast-2"

CREATE_TABLE_SQL = """
CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table} (
    market      string,
    item_name   string,
    price_nov   double,
    price_dec   double,
    change_rate double,
    change_rank int
)
STORED AS PARQUET
LOCATION '{location}'
""".format(db=DATABASE, table=TABLE, location=GOLD_LOCATION)

TOP1_SQL = """
SELECT market, item_name, price_nov, price_dec, change_rate, change_rank
FROM {db}.{table}
WHERE change_rank = 1
ORDER BY market
""".format(db=DATABASE, table=TABLE)

athena_common = dict(
    database=DATABASE,
    output_location=ATHENA_OUTPUT,
    workgroup="primary",
    aws_conn_id=None,        # instance role, no stored credentials
    region_name=REGION,
    sleep_time=3,            # poll Athena every 3 s
)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 11, 24),
    schedule=timedelta(minutes=10),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "lsw", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["de9", "week9", "q9"],
) as dag:
    spark_price_batch = SparkSubmitOperator(
        task_id="spark_price_batch",
        conn_id="spark_default",             # spark://spark-master:7077 (standalone cluster)
        application="/opt/de9/part2-price/price_batch.py",
        application_args=["--root", ROOT] if ROOT else [],
        name="price_batch",
        driver_memory="512m",                # driver runs inside the airflow container
        executor_memory="1g",
        conf={"spark.log.level": "ERROR"},
    )

    register_gold_table = AthenaOperator(
        task_id="register_gold_table",
        query=CREATE_TABLE_SQL,
        **athena_common,
    )

    query_top1 = AthenaOperator(
        task_id="query_top1",
        query=TOP1_SQL,
        **athena_common,
    )

    spark_price_batch >> register_gold_table >> query_top1
