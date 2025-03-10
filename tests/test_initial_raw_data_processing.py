import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # to get rid of warning message

import pytest
from pyspark.testing.utils import assertDataFrameEqual
from fixtures import sample_raw_data, expected_data_schema

from json import dump

from silver.raw_data_processor import RawDataProcessor

sample = sample_raw_data
schema = expected_data_schema


@pytest.fixture
def sample_data_path(tmp_path):
    return f"{tmp_path}{os.sep}data{os.sep}bronze{os.sep}weather{os.sep}Manaus"


class TestInitialRawDataProcessing:

    def test_climate_data_gathering(self, sample_data_path, tmp_path, sample, schema):
        os.makedirs(sample_data_path)
        with open(f"{sample_data_path}/sample_data.json", "w") as json:
            dump(sample, json)

        raw_data_processor = RawDataProcessor(tmp_path)

        sample_raw_data_df = raw_data_processor.read_json(str(sample_data_path))

        transformed_raw_data_df = raw_data_processor.get_climate_data_per_capital(
            sample_raw_data_df, "Manaus"
        )

        expected_data = [
            {
                "dt": "2025-03-06 08:58:13",
                "sunrise": "2025-03-06 07:06:44",
                "sunset": "2025-03-06 19:15:50",
                "temp": 24.25,
                "feels_like": 24.9,
                "pressure": 1011,
                "humidity": 83,
                "dew_point": 21.18,
                "uvi": 1.09,
                "clouds": 20,
                "visibility": 10000,
                "wind_speed": 4.12,
                "wind_deg": 90,
                "capital_name": "Manaus",
            }
        ]

        expected_df = raw_data_processor.spark.createDataFrame(
            expected_data, schema=schema
        )

        assertDataFrameEqual(transformed_raw_data_df, expected_df)
