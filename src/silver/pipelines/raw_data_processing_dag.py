from airflow.decorators import task, task_group

from silver.raw_data_processor import RawDataProcessor
from silver.raw_data_schema import schema


@task_group(group_id="Silver", ui_color="#c0c0c0", ui_fgcolor="#c0c0c0")
def base_data_processing():

    @task_group(group_id="Silver-I")
    def raw_data_processing():

        @task()
        def aggregate_climate_data():
            processor = RawDataProcessor()

            q = processor.save_climate_data_to_parquet()
            print("Saving climate data as parquet to datalake..")
            while not q.awaitTermination(1):
                status_message = q.status["message"]
                print(status_message)
                continue

            print("Climate data stored.")

        [aggregate_climate_data()]

    raw_data_processing()
