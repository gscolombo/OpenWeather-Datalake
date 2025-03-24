from typing import Literal
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    from_unixtime,
    concat_ws,
    explode,
    date_trunc,
    to_date,
    hour,
)
from json import load, dump

from .raw_data_schema import schema


class RawDataProcessor:
    spark: SparkSession

    def __init__(
        self,
        base_path: Path,
        configs: list[tuple[str, str]] = None,
        spark_session: SparkSession = None,
    ):
        self.configs = configs

        if spark_session is None:
            print("Configuring Spark session builder...")
            builder = SparkSession.builder.appName("Raw Data Initial Processing")

            if configs is not None:
                print("Applying custom configuration...")
                for key, value in configs:
                    builder.config(key, value)

            print("Starting Spark session...")
            self.spark = builder.getOrCreate()
            print("Spark session started.")
        else:
            self.spark = spark_session

        self.base_path = base_path
        self.save_path = base_path.joinpath("silver")
        self.raw_data_path = base_path.joinpath("bronze")
        self.cp_path = base_path.joinpath("checkpoints")

    def get_last_datetime(self) -> datetime | None:
        ingestion_cp = self.cp_path.joinpath("ingestion_checkpoint.json")

        if ingestion_cp.exists():
            print("Checkpoint metadata found.")
            print("Reading ingestion checkpoint...")
            with open(ingestion_cp, "r") as cp:
                last_dt = load(cp)["last_ingestion_at"]
                print(f"Last ingestion was at {last_dt}.")
                return datetime.fromisoformat(last_dt)

        print("No checkpoint metadata found.")

    def get_raw_data(self):
        print("Reading raw data...")
        raw_data = self.spark.read.json(str(self.raw_data_path), schema=schema).filter(
            col("date") == datetime.today().date()
        )

        last_dt = self.get_last_datetime()
        if last_dt is not None:
            print("Filtering out old data...")
            raw_data = raw_data.filter(from_unixtime(col("current.dt")) > last_dt)

        return raw_data

    def uts_to_dt(self, df: DataFrame, columns: list[str]):
        return df.withColumns(
            {colname: from_unixtime(colname).alias(colname) for colname in columns}
        )

    def get_climate_data(self, data: DataFrame):
        climate_data = (
            data.select(["capital", "current.*"])
            .drop("weather")
            .select(["*", "rain.1h"])
            .withColumnRenamed("1h", "rain_1h")
            .drop("rain")
            .select(["*", "snow.1h"])
            .withColumnRenamed("1h", "snow_1h")
            .drop("snow")
            .transform(self.uts_to_dt, ["dt", "sunrise", "sunset"])
            .fillna(0.0)
        )

        self.save_data_as_parquet(climate_data, "climate")

    def get_weather_data(self, data: DataFrame):
        weather_data = (
            data.select(
                [
                    "capital",
                    "current.dt",
                    data.current.weather[0].alias("weather"),
                ]
            )
            .select(
                [
                    "*",
                    "weather.id",
                    "weather.main",
                    "weather.description",
                    "weather.icon",
                ]
            )
            .drop("weather")
            .transform(self.uts_to_dt, ["dt"])
        )

        self.save_data_as_parquet(weather_data, "weather")

    def get_alert_data(self, data: DataFrame):
        alert_data = (
            data.select(["capital", "current.dt", explode("alerts").alias("alerts")])
            .select(
                [
                    "*",
                    "alerts.sender_name",
                    "alerts.event",
                    "alerts.start",
                    "alerts.end",
                    "alerts.description",
                    "alerts.tags",
                ]
            )
            .drop("alerts")
            .withColumn("tags", concat_ws(" ", "tags"))
            .transform(self.uts_to_dt, ["dt", "start", "end"])
        )

        self.save_data_as_parquet(alert_data, "alert")

    def save_data_as_parquet(
        self,
        data: DataFrame,
        dataset: Literal["climate", "weather", "alert"],
    ):
        print(f"Saving {dataset} data...")

        dataset_save_path = self.save_path.joinpath(dataset)
        dataset_save_path.mkdir(parents=True, exist_ok=True)

        (
            data.withColumns(
                {
                    "day": to_date(date_trunc("day", "dt")),
                    "hour": hour("dt"),
                }
            )
            .write.partitionBy(
                "capital",
                "day",
                "hour",
            )
            .mode("append")
            .parquet(str(dataset_save_path))
        )

    def update_ingestion_checkpoint(self, dt: str):
        path = Path(self.base_path, "checkpoints")

        path.mkdir(parents=True, exist_ok=True)

        print("Updating ingestion checkpoint")
        with open(path.joinpath(f"ingestion_checkpoint.json"), "w") as f:
            dump({"last_ingestion_at": dt}, f)
