from airflow.decorators import task, task_group

from bronze.capital_coords import capital_coords
from bronze.raw_data_ingestion import request_data, save_data


@task_group(group_id="Bronze", ui_color="#cd7f32", ui_fgcolor="#cd7f32")
def raw_data_ingestion():

    @task()
    def request_weather_data():
        data = {}
        for capital in capital_coords:
            print(f"Requesting weather data for capital {capital}")
            data[capital] = request_data(capital)
            print("Data received.")

        return data

    @task()
    def save_data_on_success(responses: dict[dict]):
        for capital in responses:
            status_code, data = responses[capital].values()
            if status_code != 200:
                error = f"""
                Error requesting data:
                Status Code: {status_code}
                Response: {data}
                """
                print(error)
            else:
                print(f"Saving weather data for {capital}")
                save_data(data, capital, "datalake")

    save_data_on_success(request_weather_data())
