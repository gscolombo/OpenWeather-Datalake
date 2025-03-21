import os
from pathlib import Path
from typing import Literal
from airflow.decorators import task, task_group
from pyspark.sql import SparkSession

from silver.raw_data_processor import RawDataProcessor


DATALAKE_BASE_PATH = Path("datalake", "silver")


def aggregate_data(dataset: Literal["climate", "weather", "alert"]):
    raw_data_path = Path("datalake", "bronze", "weather")
    if not os.path.exists("./datalake/bronze/weather"):
        raise FileNotFoundError("Raw data not stored.")

    capitals = os.listdir(raw_data_path)

    save_path = DATALAKE_BASE_PATH.joinpath(dataset)
    save_path.mkdir(parents=True, exist_ok=True)

    save_path_cp = save_path.joinpath("checkpoint")

    processor = RawDataProcessor(raw_data_path, capitals)
    print(f"Saving {dataset} data as parquet to datalake..")
    q = processor.save_data_stream_to_parquet(
        save_path.as_posix(), save_path_cp.as_posix(), dataset
    )
    while not q.awaitTermination(5):
        status_message = q.status["message"]
        print(status_message)
        continue

    print(f"{dataset.capitalize()} data stored.")


@task_group(group_id="Silver", ui_color="#c0c0c0", ui_fgcolor="#c0c0c0")
def raw_data_processing():

    @task()
    def aggregate_climate_data():
        aggregate_data("climate")

    @task
    def aggregate_weather_data():
        aggregate_data("weather")

    @task
    def aggregate_alert_data():
        aggregate_data("alert")

    [
        aggregate_climate_data(),
        aggregate_weather_data(),
        aggregate_alert_data(),
    ]
