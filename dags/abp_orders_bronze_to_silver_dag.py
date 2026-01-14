from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="abp_orders_bronze_to_silver",
    description="Bronze → Silver CDC merge for AbpOrders",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",  # every 10 minutes
    catchup=False,
    max_active_runs=1,
    tags=["delta", "cdc", "silver", "spark"],
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="./jobs/abp_orders_bronze_to_silver.py",
        conn_id="spark_default",
        name="abp-orders-bronze-to-silver",
        verbose=True,

        executor_cores=2,
        executor_memory="4g",
        num_executors=2,
        driver_memory="2g",

        conf={
            # Delta
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",

            # MinIO / S3
            "spark.hadoop.fs.s3a.endpoint": "http://192.168.2.70:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )

    bronze_to_silver
