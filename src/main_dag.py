from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.decorators import task
from pyspark.sql import SparkSession

from default_args import default_args
from bronze.pipelines.data_ingestion_dag import raw_data_ingestion
from silver.pipelines.raw_data_processing_dag import (
    raw_data_processing,
    DATALAKE_BASE_PATH,
)

with DAG(
    dag_id="main",
    dag_display_name="Brazil Weather Data Lakehouse",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    @task(retries=0)
    def output_climate_data():
        spark: SparkSession = SparkSession.builder.getOrCreate()
        spark.read.parquet(str(DATALAKE_BASE_PATH.joinpath("silver", "climate"))).show()

    @task(retries=0)
    def output_weather_data():
        spark: SparkSession = SparkSession.builder.getOrCreate()
        spark.read.parquet(str(DATALAKE_BASE_PATH.joinpath("silver", "weather"))).show()

    @task(retries=0)
    def output_alert_data():
        spark: SparkSession = SparkSession.builder.getOrCreate()
        spark.read.parquet(str(DATALAKE_BASE_PATH.joinpath("silver", "alert"))).show()

    (
        raw_data_ingestion()
        >> raw_data_processing()
        >> [output_climate_data(), output_weather_data(), output_alert_data()]
    )
