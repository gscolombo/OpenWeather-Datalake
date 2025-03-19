from pyspark.sql.types import *

schema = StructType(
    [
        StructField(
            "alerts",
            ArrayType(
                StructType(
                    [
                        StructField("description", StringType(), True),
                        StructField("end", LongType(), True),
                        StructField("event", StringType(), True),
                        StructField("sender_name", StringType(), True),
                        StructField("start", LongType(), True),
                        StructField("tags", ArrayType(StringType(), True), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "current",
            StructType(
                [
                    StructField("clouds", LongType(), True),
                    StructField("dew_point", DoubleType(), True),
                    StructField("dt", LongType(), True),
                    StructField("feels_like", DoubleType(), True),
                    StructField("humidity", LongType(), True),
                    StructField("pressure", LongType(), True),
                    StructField("sunrise", LongType(), True),
                    StructField("sunset", LongType(), True),
                    StructField("temp", DoubleType(), True),
                    StructField("uvi", DoubleType(), True),
                    StructField("visibility", LongType(), True),
                    StructField(
                        "weather",
                        ArrayType(
                            StructType(
                                [
                                    StructField("description", StringType(), True),
                                    StructField("icon", StringType(), True),
                                    StructField("id", LongType(), True),
                                    StructField("main", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        True,
                    ),
                    StructField("wind_deg", LongType(), True),
                    StructField("wind_speed", DoubleType(), True),
                    StructField("wind_gust", DoubleType(), True),
                    StructField(
                        "rain",
                        StructType([StructField("1h", DoubleType(), True)]),
                        True,
                    ),
                    StructField(
                        "snow",
                        StructType([StructField("1h", DoubleType(), True)]),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("timezone_offset", LongType(), True),
    ]
)
