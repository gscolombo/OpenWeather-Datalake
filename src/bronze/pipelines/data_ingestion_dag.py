from airflow.decorators import task, task_group

from bronze.capital_coords import capital_coords
from bronze.raw_data_ingestion import request_data, save_data


@task_group(group_id="Bronze", ui_color="#cd7f32", ui_fgcolor="#cd7f32")
def raw_data_ingestion():

    @task
    def request_weather_data():
        data = {}
        for capital in capital_coords:
            print(f"Requesting weather data for capital {capital}")
            res = request_data(capital)
            if res["status_code"] != 200:
                error = f"""
                Error requesting data:
                Status Code: {res["status_code"]}
                Response: {res["data"]}
                """
                raise RuntimeError(error)

            data[capital] = res["data"]
            print("Data received.")

        return data

    @task
    def save_data_on_success(data: dict[dict]):
        for capital in data:
            print(f"Saving weather data for {capital}")
            save_data(data[capital], capital, "datalake")

    save_data_on_success(request_weather_data())
