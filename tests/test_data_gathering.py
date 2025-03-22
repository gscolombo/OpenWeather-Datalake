import os
from pathlib import Path
from datetime import datetime

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # to get rid of warning message

import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.functions import col
from fixtures import *


from bronze.raw_data_ingestion import save_data, update_ingestion_checkpoint
from silver.raw_data_processor import RawDataProcessor
from silver.raw_data_schema import schema


@pytest.fixture()
def raw_data_path(tmp_path):
    return Path(tmp_path, "bronze")


@pytest.fixture()
def processor(tmp_path, sample_raw_data_Manaus, sample_raw_data_Brasilia):
    save_data(sample_raw_data_Brasilia, "Brasilia", tmp_path)
    save_data(sample_raw_data_Manaus, "Manaus", tmp_path)

    return RawDataProcessor(tmp_path)


@pytest.fixture()
def processor_with_cp(tmp_path, sample_raw_data_Brasilia, sample_raw_data_Manaus):
    save_data(sample_raw_data_Manaus, "Manaus", tmp_path)

    dt = datetime.fromtimestamp(sample_raw_data_Manaus[0]["current"]["dt"]).isoformat()
    update_ingestion_checkpoint(dt, tmp_path)

    save_data(sample_raw_data_Brasilia, "Brasilia", tmp_path)

    return RawDataProcessor(tmp_path)


class TestRawDataProcessing:

    def test_raw_data_loading(self, processor: RawDataProcessor, raw_data_path):
        expected_df = processor.spark.read.json(raw_data_path.as_posix(), schema=schema)

        assertDataFrameEqual(processor.raw_data, expected_df)

    def test_checkpoint_filtered_loading(
        self,
        processor_with_cp: RawDataProcessor,
        raw_data_path,
    ):
        expected_df = processor_with_cp.spark.read.json(
            str(raw_data_path), schema=schema
        ).filter(col("capital") == "Brasilia")

        assertDataFrameEqual(processor_with_cp.raw_data, expected_df)

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
