from pytest import fixture
from pyspark.sql.types import *


@fixture
def sample_raw_data():
    return [
        {
            "lat": -3.1316,
            "lon": -59.9825,
            "timezone": "America/Manaus",
            "timezone_offset": -14400,
            "current": {
                "dt": 1741262293,
                "sunrise": 1741255604,
                "sunset": 1741299350,
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
                "weather": [
                    {
                        "id": 801,
                        "main": "Clouds",
                        "description": "algumas nuvens",
                        "icon": "02d",
                    }
                ],
            },
            "alerts": [
                {
                    "sender_name": "Instituto Nacional de Meteorologia",
                    "event": "Chuvas Intensas",
                    "start": 1741178700,
                    "end": 1741266000,
                    "description": "INMET publica aviso iniciando em: 05/03/2025 09:45. Chuva entre 30 e 60 mm/h ou 50 e 100 mm/dia, ventos intensos (60-100 km/h). Risco de corte de energia el\u00e9trica,  queda de galhos de \u00e1rvores, alagamentos e de descargas el\u00e9tricas.",
                    "tags": ["Rain"],
                }
            ],
        }
    ]


@fixture
def expected_data_schema():
    return StructType(
        [
            StructField("clouds", LongType(), True),
            StructField("dew_point", DoubleType(), True),
            StructField("dt", StringType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("humidity", LongType(), True),
            StructField("pressure", LongType(), True),
            StructField("sunrise", StringType(), True),
            StructField("sunset", StringType(), True),
            StructField("temp", DoubleType(), True),
            StructField("uvi", DoubleType(), True),
            StructField("visibility", LongType(), True),
            StructField("wind_deg", LongType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("capital_name", StringType(), False),
        ]
    )
