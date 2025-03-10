import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, from_unixtime
from functools import reduce
from .raw_data_schema import schema

ROOT_PATH = os.path.abspath(__file__).split("/src")[0]


class RawDataProcessor:

    def __init__(self):
        self.spark = SparkSession.builder.appName(
            "Raw Data Initial Processing"
        ).getOrCreate()

        self.path = f"{ROOT_PATH}/data/bronze/weather"
        self.capitals = os.listdir(self.path)

    def read_json(self, path: str):
        return self.spark.read.json(path, schema=schema, multiLine=True)

    def read_json_stream(self, path: str):
        return self.spark.readStream.json(path, schema=schema, multiLine=True)

    def get_data_per_capital(self):
        return {
            capital: self.read_json_stream(f"{self.path}/{capital}")
            for capital in self.capitals
        }

    def get_climate_data_per_capital(self, df: DataFrame, capital: str):
        uts_to_dt = lambda df: df.withColumns(
            {
                colname: from_unixtime(colname).alias(colname)
                for colname in ["dt", "sunrise", "sunset"]
            }
        )

        return (
            df.select("current.*")
            .drop("weather")
            .withColumn("capital_name", lit(capital))
            .transform(uts_to_dt)
        )

    def get_climate_data(self):
        data = self.get_data_per_capital()

        climate_data = [
            self.get_climate_data_per_capital(data[capital], capital)
            for capital in data
        ]

        return reduce(lambda a, b: a.union(b), climate_data)

    def save_climate_data_to_parquet(self):
        save_path = f"{ROOT_PATH}/data/silver/climate"
        if not os.path.exists(save_path):
            os.mkdir(save_path)
            os.mkdir(save_path + "/checkpoint")

        climate_data = self.get_climate_data()

        climate_data.writeStream.trigger(availableNow=True).format("parquet").option(
            "checkpointLocation", save_path + "/checkpoint"
        ).start(save_path)
