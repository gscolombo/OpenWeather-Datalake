from datetime import datetime, timedelta

from airflow.models.dag import DAG
from default_args import default_args

from bronze.pipelines.data_ingestion_dag import raw

with DAG(
    dag_id="main",
    dag_display_name="OpenWeather Current Weather Data Lakehouse",
    default_args=default_args,
    schedule=timedelta(minutes=10),
    start_date=datetime.today()
) as dag:
    
    raw()