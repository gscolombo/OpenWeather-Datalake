import os
from pathlib import Path

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # to get rid of warning message

import pytest
from pyspark.testing.utils import assertDataFrameEqual
from fixtures import *

from json import dump

from silver.raw_data_processor import RawDataProcessor


@pytest.fixture
def processor(tmp_path, sample_raw_data_Manaus, sample_raw_data_Brasilia):
    raw_data_path = Path(tmp_path, "bronze", "weather")

    ma_path = raw_data_path.joinpath("Manaus")
    ma_path.mkdir(parents=True)
    with open(f"{ma_path}/sample_data.json", "w") as json:
        dump(sample_raw_data_Manaus, json)

    bsb_path = raw_data_path.joinpath("Brasilia")
    bsb_path.mkdir(parents=True)
    with open(f"{bsb_path}/sample_data.json", "w") as json:
        dump(sample_raw_data_Brasilia, json)

    return RawDataProcessor(
        raw_data_path, streaming=False, capitals=["Manaus", "Brasilia"]
    )


class TestInitialRawDataProcessing:

    def test_climate_data_gathering(
        self,
        processor: RawDataProcessor,
        expected_climate_data,
        expected_climate_data_schema,
    ):

        transformed_raw_data_df = processor.gather_data("climate")

        expected_df = processor.spark.createDataFrame(
            expected_climate_data, schema=expected_climate_data_schema
        ).fillna(0.0)

        assertDataFrameEqual(transformed_raw_data_df, expected_df)

    def test_weather_data_gathering(
        self,
        processor: RawDataProcessor,
        expected_weather_data,
        expected_weather_data_schema,
    ):
        transformed_raw_data_df = processor.gather_data("weather")

        expected_df = processor.spark.createDataFrame(
            expected_weather_data, schema=expected_weather_data_schema
        )

        assertDataFrameEqual(transformed_raw_data_df, expected_df)

    def test_alert_data_gathering(
        self,
        processor: RawDataProcessor,
        expected_alert_data,
        expected_alert_data_schema,
    ):
        transformed_raw_data_df = processor.gather_data("alert")

        expected_df = processor.spark.createDataFrame(
            expected_alert_data, schema=expected_alert_data_schema
        )

        assertDataFrameEqual(transformed_raw_data_df, expected_df)
