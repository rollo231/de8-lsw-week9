# de9-airflow:2.10.5 — Airflow image that can submit jobs to the Spark standalone cluster.
#
# Why these choices:
# - python3.8 base: the Spark cluster image (apache/spark:3.5.3) runs Python 3.8.10.
#   In client mode the PySpark driver runs HERE and the executors run on the worker,
#   so driver and worker must share the same Python minor version.
# - Java 11 + the exact Spark 3.5.3 distribution are copied from the cluster image,
#   so spark-submit here is byte-identical to the cluster's.
# - Same extra jars as the cluster image (Kafka source + S3A) — see part1-ev/docker-compose.yaml.
# - apache-airflow-providers-apache-spark gives SparkSubmitOperator (Q9);
#   the Amazon provider (Athena) is already in the base image.
FROM apache/airflow:2.10.5-python3.8

USER root
COPY --from=apache/spark:3.5.3 /opt/java/openjdk /opt/java/openjdk
COPY --from=apache/spark:3.5.3 /opt/spark /opt/spark
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:/opt/java/openjdk/bin:${PATH}"

USER airflow
# Official constraints for Airflow 2.10.5 / Python 3.8, with pyspark pinned to the
# cluster version (3.5.3) instead of the constraint's 3.5.4.
RUN curl -fsSL https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt \
      | sed 's/^pyspark==.*/pyspark==3.5.3/' > /tmp/constraints.txt \
 && pip install --no-cache-dir -c /tmp/constraints.txt \
      "apache-airflow==2.10.5" \
      "apache-airflow-providers-apache-spark==4.11.0" \
      "pyspark==3.5.3" \
 && rm /tmp/constraints.txt
