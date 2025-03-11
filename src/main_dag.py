from datetime import datetime, timedelta

from airflow.models.dag import DAG
from default_args import default_args

from bronze.pipelines.data_ingestion_dag import raw
from silver.pipelines.raw_data_processing_dag import base_data_processing

with DAG(
    dag_id="main",
    dag_display_name="Brazil Weather Data Lakehouse",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    start_date=datetime.today(),
    catchup=False,
) as dag:

    raw() >> base_data_processing()
