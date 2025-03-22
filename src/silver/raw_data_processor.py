from typing import Literal
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_unixtime, concat_ws, explode, date_trunc
from json import load

from .raw_data_schema import schema


class RawDataProcessor:
    spark: SparkSession

    def __init__(
        self,
        base_path: Path,
        configs: list[tuple[str, str]] = None,
    ):
        print("Configuring Spark session builder...")
        builder = SparkSession.builder.appName("Raw Data Initial Processing")

        if configs is not None:
            print("Applying custom configuration...")
            for key, value in configs:
                builder.config(key, value)

        print("Starting Spark session...")
        self.spark = builder.getOrCreate()
        print("Spark session started.")

        self.save_path = base_path.joinpath("silver")
        self.raw_data_path = base_path.joinpath("bronze")
        self.cp_path = base_path.joinpath("checkpoints")

        self.raw_data = self.get_raw_data()

    def get_last_datetime(self) -> str | None:
        ingestion_cp = self.cp_path.joinpath("ingestion_checkpoint.json")

        if ingestion_cp.exists():
            print("Checkpoint metadata found.")
            print("Reading ingestion checkpoint...")
            with open(ingestion_cp, "r") as cp:
                last_dt = load(cp)["last_ingestion_at"]
                print(f"Last ingestion was at {last_dt}.")
                return last_dt

        print("No checkpoint metadata found.")

    def get_raw_data(self):
        print("Reading raw data...")
        raw_data = self.spark.read.json(str(self.raw_data_path), schema=schema)

        last_dt = self.get_last_datetime()
        if last_dt is not None:
            print("Filtering out old data...")
            raw_data = raw_data.filter(from_unixtime(col("current.dt")) > last_dt)

        return raw_data

    def uts_to_dt(self, df: DataFrame, columns: list[str]):
        return df.withColumns(
            {colname: from_unixtime(colname).alias(colname) for colname in columns}
        )

    def get_climate_data(self):
        return (
            self.raw_data.select(["capital", "current.*"])
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

    def get_weather_data(self):
        return (
            self.raw_data.select(
                [
                    "capital",
                    "current.dt",
                    self.raw_data.current.weather[0].alias("weather"),
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

    def get_alert_data(self):
        return (
            self.raw_data.select(
                ["capital", "current.dt", explode("alerts").alias("alerts")]
            )
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

    def gather_data(
        self,
        dataset: Literal["climate", "weather", "alert"],
    ) -> DataFrame:
        fn = {
            "climate": self.get_climate_data,
            "weather": self.get_weather_data,
            "alert": self.get_alert_data,
        }

        return fn[dataset]()

    def save_data_as_parquet(
        self,
        dataset: Literal["climate", "weather", "alert"],
    ):
        data = self.gather_data(dataset)

        print(f"Saving {dataset} data...")
        data.write.partitionBy(
            dataset, "capital", date_trunc("day", "dt"), date_trunc("hour", "dt")
        ).mode("append").parquet(str(self.save_path))
