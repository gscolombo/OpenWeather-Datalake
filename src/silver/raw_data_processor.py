import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, from_unixtime
from pyspark.sql.streaming.query import StreamingQuery
from functools import reduce
from dotenv import load_dotenv

from .raw_data_schema import schema

load_dotenv

ROOT_PATH = os.getenv("ROOT_PATH")


class RawDataProcessor:

    def __init__(self, root: str = ROOT_PATH):
        self.root = root
        self.path = f"{root}/data/bronze/weather"
        assert os.path.exists(self.path)

        self.spark = SparkSession.builder.appName(
            "Raw Data Initial Processing"
        ).getOrCreate()

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

    def save_climate_data_to_parquet(self) -> StreamingQuery:
        save_path = f"{self.root}/data/silver/climate"
        if not os.path.exists(save_path):
            os.makedirs(save_path, mode=0o777)
            os.mkdir(save_path + "/checkpoint")

        climate_data = self.get_climate_data()

        q = (
            climate_data.writeStream.trigger(availableNow=True)
            .format("parquet")
            .option("checkpointLocation", save_path + "/checkpoint")
            .start(save_path)
        )

        return q
