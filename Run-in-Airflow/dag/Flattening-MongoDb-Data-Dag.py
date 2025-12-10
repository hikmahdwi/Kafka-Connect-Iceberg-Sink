import datetime
from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
  dag_id="transform_iceberg_flat_redeem",
  start_date=datetime.datetime(2025, 7, 14),
  schedule="@once",
):
  SparkSubmitOperator(
    task_id="transform_iceberg_flat_redeem_task",
    application="jobs/job_transform_iceberg_flat_redeem_task.py",
    conn_id="spark_default",
    # jars="jobs/jar/hudi-utilities-bundle_2.12-1.0.2.jar",
    packages=(
      'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,' 
      'org.apache.hadoop:hadoop-aws:3.3.4,'
      'com.amazonaws:aws-java-sdk-bundle:1.12.262'
    )
  ) 
