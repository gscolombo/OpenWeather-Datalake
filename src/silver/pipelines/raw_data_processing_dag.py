import os
from pathlib import Path
from datetime import datetime

from airflow.decorators import task, task_group

from silver.raw_data_processor import RawDataProcessor

DATALAKE_BASE_PATH = Path("datalake")


@task_group(group_id="Silver", ui_color="#c0c0c0", ui_fgcolor="#c0c0c0")
def raw_data_processing():
    processor = RawDataProcessor(base_path=DATALAKE_BASE_PATH)
    tmp_path = DATALAKE_BASE_PATH.joinpath("tmp")

    def read_raw_data():
        return processor.spark.read.parquet(str(tmp_path))

    @task(retries=0)
    def get_last_raw_data():
        dt = datetime.now().replace(second=59, microsecond=0).isoformat()

        raw_data = processor.get_raw_data()
        processor.update_ingestion_checkpoint(dt)
        raw_data.write.parquet(str(tmp_path))

    @task()
    def aggregate_climate_data():
        data = read_raw_data()
        processor.get_climate_data(data)

    @task()
    def aggregate_weather_data():
        data = read_raw_data()
        processor.get_weather_data(data)

    @task()
    def aggregate_alert_data():
        data = read_raw_data()
        processor.get_alert_data(data)

    @task(retries=0)
    def clean_up_temporary_path():
        print("Cleaning up temporary stored raw data...")
        for file in os.listdir(tmp_path):
            os.remove(tmp_path.joinpath(file))
        tmp_path.rmdir()

    (
        get_last_raw_data()
        >> [
            aggregate_climate_data(),
            aggregate_weather_data(),
            aggregate_alert_data(),
        ]
        >> clean_up_temporary_path()
    )
