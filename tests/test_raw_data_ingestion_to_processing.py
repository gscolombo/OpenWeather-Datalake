import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # to get rid of warning message

import pytest
from pyspark.testing.utils import assertDataFrameEqual
from fixtures import sample_raw_data, expected_data_schema

from bronze.raw_data_ingestion import save_data
from silver.raw_data_processor import RawDataProcessor

sample = sample_raw_data
schema = expected_data_schema


@pytest.fixture
def bronze_test_dir(tmp_path):
    return f"{tmp_path}{os.sep}data{os.sep}bronze{os.sep}weather{os.sep}Manaus"


@pytest.fixture
def silver_test_dir(tmp_path):
    return f"{tmp_path}{os.sep}data{os.sep}silver{os.sep}climate"


class TestRawDataIngestionToProcessing:

    def test_data_ingestion_to_processing(
        self, tmp_path, bronze_test_dir, silver_test_dir, sample, schema
    ):
        save_data(sample, "Manaus", tmp_path)

        assert os.path.exists(bronze_test_dir) and len(os.listdir(bronze_test_dir)) > 0

        raw_data_processor = RawDataProcessor(tmp_path)
        query = raw_data_processor.save_climate_data_to_parquet()

        while not query.awaitTermination(5):
            continue

        assert os.path.exists(silver_test_dir) and len(os.listdir(silver_test_dir)) > 0

        transformed_df = raw_data_processor.spark.read.parquet(
            silver_test_dir, schema=schema
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

        expected_data_path = f"{tmp_path}silver_test_dir{os.sep}expected"

        raw_data_processor.spark.createDataFrame(
            expected_data, schema=schema
        ).write.parquet(expected_data_path)

        expected_df = raw_data_processor.spark.read.parquet(
            expected_data_path, schema=schema
        )

        assertDataFrameEqual(transformed_df, expected_df)
