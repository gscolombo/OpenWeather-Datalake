import os
from typing import Literal
from pathlib import Path
from datetime import datetime
from json import dumps

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # to get rid of warning message

import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.functions import col
from fixtures import *


from bronze.raw_data_ingestion import save_data
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
    processor = RawDataProcessor(tmp_path)

    processor.update_ingestion_checkpoint(dt)

    save_data(sample_raw_data_Brasilia, "Brasilia", tmp_path)

    return processor


class TestRawDataProcessing:

    def test_raw_data_loading(self, processor: RawDataProcessor, raw_data_path):
        raw_data = processor.get_raw_data()
        raw_data.explain(mode="formatted")
        expected_df = processor.spark.read.json(raw_data_path.as_posix(), schema=schema)

        assertDataFrameEqual(raw_data, expected_df)

    def test_checkpoint_creation(self, tmp_path: Path, processor: RawDataProcessor):
        now = datetime.now().isoformat()

        processor.update_ingestion_checkpoint(now)

        cp_path = Path(tmp_path, "checkpoints")
        assert os.path.exists(cp_path)

        cp = os.listdir(cp_path)[-1]
        with open(cp_path.joinpath(cp), "r") as f:
            assert f.read() == dumps({"last_ingestion_at": now})

    def test_checkpoint_filtered_loading(
        self,
        processor_with_cp: RawDataProcessor,
        raw_data_path,
    ):
        raw_data = processor_with_cp.get_raw_data()
        raw_data.explain(mode="formatted")

        expected_df = processor_with_cp.spark.read.json(
            str(raw_data_path), schema=schema
        ).filter(col("capital") == "Brasilia")

        assertDataFrameEqual(raw_data, expected_df)

    def _partition_testing(
        self, root: Path, dataset: Literal["climate", "weather", "alert"]
    ):
        manaus_partitions = Path(
            root,
            "silver",
            dataset,
            "capital=Manaus",
            "day=2025-03-06",
            "hour=8",
        )

        bsb_partitions = Path(
            root,
            "silver",
            dataset,
            "capital=Brasilia",
            "day=2025-03-18",
            "hour=21",
        )
        assert manaus_partitions.exists() and bsb_partitions.exists()

    def test_climate_data_gathering(
        self,
        tmp_path,
        processor: RawDataProcessor,
        expected_climate_data,
        expected_climate_data_schema,
    ):

        raw_data = processor.get_raw_data()
        processor.get_climate_data(raw_data)

        self._partition_testing(tmp_path, "climate")

        data_path = Path(tmp_path, "silver", "climate").as_posix()
        actual_df = processor.spark.read.parquet(data_path)

        expected_df = processor.spark.createDataFrame(
            expected_climate_data, schema=expected_climate_data_schema
        ).fillna(0.0)

        assertDataFrameEqual(actual_df, expected_df)

    def test_weather_data_gathering(
        self,
        tmp_path,
        processor: RawDataProcessor,
        expected_weather_data,
        expected_weather_data_schema,
    ):
        raw_data = processor.get_raw_data()
        processor.get_weather_data(raw_data)

        self._partition_testing(tmp_path, "weather")

        data_path = Path(tmp_path, "silver", "weather").as_posix()
        actual_df = processor.spark.read.parquet(data_path)

        expected_df = processor.spark.createDataFrame(
            expected_weather_data, schema=expected_weather_data_schema
        )

        assertDataFrameEqual(actual_df, expected_df)

    def test_alert_data_gathering(
        self,
        tmp_path,
        processor: RawDataProcessor,
        expected_alert_data,
        expected_alert_data_schema,
    ):
        raw_data = processor.get_raw_data()
        processor.get_alert_data(raw_data)

        self._partition_testing(tmp_path, "alert")

        data_path = Path(tmp_path, "silver", "alert").as_posix()
        actual_df = processor.spark.read.parquet(data_path)

        expected_df = processor.spark.createDataFrame(
            expected_alert_data, schema=expected_alert_data_schema
        )

        assertDataFrameEqual(actual_df, expected_df)
