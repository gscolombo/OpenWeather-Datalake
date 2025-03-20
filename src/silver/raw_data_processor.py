from typing import Literal

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, from_unixtime, concat_ws, explode
from pyspark.sql.streaming.query import StreamingQuery
from functools import reduce

from .raw_data_schema import schema


class RawDataProcessor:
    spark: SparkSession

    def __init__(
        self,
        raw_data_path: str,
        capitals: list[str],
        configs: list[tuple[str, str]] = None,
        streaming=True,
    ):
        self.streaming = streaming
        self.raw_data_path = raw_data_path
        self.capitals = capitals

        print("Configuring Spark session builder...")
        self.builder = SparkSession.builder.appName("Raw Data Initial Processing")

        if configs is not None:
            print("Applying custom configuration...")
            for key, value in configs:
                self.builder.config(key, value)

        print("Starting Spark session...")
        self.spark = self.builder.getOrCreate()
        print("Spark session started.")

        self.raw_data = self.get_data_per_capital()

    def read_json(self, path: str):
        return self.spark.read.json(path, schema=schema)

    def read_json_stream(self, path: str):
        return self.spark.readStream.json(path, schema=schema)

    def get_data_per_capital(self):
        data = {}

        reader = self.read_json_stream if self.streaming else self.read_json

        for capital in self.capitals:
            print(f"Reading data for {capital}...")
            data[capital] = reader(f"{self.raw_data_path}/{capital}")

        return data

    def uts_to_dt(self, df: DataFrame, columns: list[str]):
        return df.withColumns(
            {colname: from_unixtime(colname).alias(colname) for colname in columns}
        )

    def get_climate_data_per_capital(self, df: DataFrame, capital: str):
        return (
            df.select(["lat", "lon", "current.*"])
            .drop("weather")
            .select(["*", "rain.1h"])
            .withColumnRenamed("1h", "rain_1h")
            .drop("rain")
            .select(["*", "snow.1h"])
            .withColumnRenamed("1h", "snow_1h")
            .drop("snow")
            .withColumn("capital_name", lit(capital))
            .transform(self.uts_to_dt, ["dt", "sunrise", "sunset"])
        )

    def get_weather_data_per_capital(self, df: DataFrame, capital: str):
        return (
            df.select(
                ["lat", "lon", "current.dt", df.current.weather[0].alias("weather")]
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
            .withColumn("capital_name", lit(capital))
        )

    def get_alert_data_per_capital(self, df: DataFrame, capital: str):
        return (
            df.select(["lat", "lon", "current.dt", explode("alerts").alias("alerts")])
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
            .withColumn("capital_name", lit(capital))
            .withColumn("tags", concat_ws(" ", "tags"))
            .transform(self.uts_to_dt, ["dt", "start", "end"])
        )

    def gather_data(self, dataset: Literal["climate", "weather", "alert"]):
        fn = {
            "climate": self.get_climate_data_per_capital,
            "weather": self.get_weather_data_per_capital,
            "alert": self.get_alert_data_per_capital,
        }

        print(f"Gathering {dataset} data...")
        data_per_capital = [
            fn[dataset](self.raw_data[capital], capital) for capital in self.raw_data
        ]

        return reduce(lambda a, b: a.union(b), data_per_capital)

    def save_data_stream_to_parquet(
        self,
        save_path: str,
        checkpoint_path: str,
        dataset: Literal["climate", "weather", "alert"],
    ) -> StreamingQuery:
        data = self.gather_data(dataset)

        return (
            data.writeStream.trigger(availableNow=True)
            .format("parquet")
            .option("checkpointLocation", checkpoint_path)
            .start(save_path, outputMode="append")
        )
