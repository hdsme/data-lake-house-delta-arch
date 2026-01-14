from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="abp_orders_cdc_stream",
    default_args=default_args,
    description="CDC AbpOrders Kafka → Delta Bronze (5 min)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,  # VERY IMPORTANT for streaming jobs
    tags=["spark", "kafka", "delta", "cdc"],
) as dag:

    spark_stream_job = SparkSubmitOperator(
        task_id="spark_abp_orders_stream",
        application="/opt/spark/jobs/abp_orders_stream.py",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

            "spark.hadoop.fs.s3a.endpoint": "http://192.168.2.70:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

            "spark.databricks.delta.optimizeWrite.enabled": "false",
            "spark.databricks.delta.autoCompact.enabled": "false",
        },
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=2,
        name="abp-orders-cdc-stream",
    )

    spark_stream_job
