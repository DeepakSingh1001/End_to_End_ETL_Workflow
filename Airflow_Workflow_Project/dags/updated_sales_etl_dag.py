import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Make scripts folder importable
sys.path.append("/opt/airflow/scripts")

from validate_data import validate_all_files

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="s3_sales_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["s3", "pyspark", "etl"],
) as dag:

    # -------------------------
    # Task 1: Validate Data
    # -------------------------

    validate_data = PythonOperator(
        task_id="validate_raw_s3_data", python_callable=validate_all_files
    )

    # -------------------------
    # Task 2: PySpark Transform
    # -------------------------
    spark_transform = BashOperator(
        task_id="spark_transform_data",
        bash_command="""
        spark-submit \
        --conf spark.driver.memory=512m \
        --conf spark.executor.memory=512m \
        --conf spark.sql.shuffle.partitions=4 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
        /opt/airflow/scripts/spark_transform.py
        """,
    )

    def run_existing_glue_crawler():
        hook = GlueCrawlerHook(aws_conn_id="aws_default")
        crawler_name = "deepsk_etl_crawler_pipeline"
        hook.start_crawler(crawler_name)
        hook.wait_for_crawler_completion(crawler_name)

    run_glue_crawler = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=run_existing_glue_crawler,
    )

    validate_data >> spark_transform >> run_glue_crawler
