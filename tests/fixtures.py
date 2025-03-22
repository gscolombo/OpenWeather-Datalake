from pytest import fixture
from pyspark.sql.types import *


@fixture()
def sample_raw_data_Manaus():
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
                    "tags": ["Rain", "Storm"],
                },
                {
                    "sender_name": "Instituto Nacional de Meteorologia",
                    "event": "Chuvas Muito Intensas",
                    "start": 1741178700,
                    "end": 1741266000,
                    "description": "INMET publica aviso iniciando em: 05/03/2025 09:45. Chuva entre 30 e 60 mm/h ou 50 e 100 mm/dia, ventos intensos (60-100 km/h). Risco de corte de energia el\u00e9trica,  queda de galhos de \u00e1rvores, alagamentos e de descargas el\u00e9tricas.",
                    "tags": ["Rain"],
                },
            ],
        },
    ]


@fixture
def sample_raw_data_Brasilia():
    return [
        {
            "lat": -15.7934,
            "lon": -47.8823,
            "timezone": "America/Sao_Paulo",
            "timezone_offset": -10800,
            "current": {
                "dt": 1742343960,
                "sunrise": 1742289309,
                "sunset": 1742333036,
                "temp": 25.78,
                "feels_like": 25.9,
                "pressure": 1013,
                "humidity": 57,
                "dew_point": 16.62,
                "uvi": 0,
                "clouds": 0,
                "visibility": 10000,
                "wind_speed": 2.06,
                "wind_deg": 200,
                "weather": [
                    {
                        "id": 500,
                        "main": "Rain",
                        "description": "chuva leve",
                        "icon": "10n",
                    }
                ],
                "rain": {"1h": 0.13},
            },
            "alerts": [
                {
                    "sender_name": "Instituto Nacional de Meteorologia",
                    "event": "Baixa Umidade",
                    "start": 1741178700,
                    "end": 1741266000,
                    "description": "Baixa umidade publicada pelo INMET",
                    "tags": ["Dry"],
                },
                {
                    "sender_name": "Instituto Nacional de Meteorologia",
                    "event": "Seca Extrema",
                    "start": 1741178700,
                    "end": 1741266000,
                    "description": "Seca Extrema publicada pelo INMET",
                    "tags": ["Drought"],
                },
            ],
        },
    ]


@fixture
def expected_climate_data_schema():
    return StructType(
        [
            StructField("capital", StringType(), False),
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
            StructField("wind_gust", DoubleType(), True),
            StructField("rain_1h", DoubleType(), True),
            StructField("snow_1h", DoubleType(), True),
        ]
    )


@fixture
def expected_weather_data_schema():
    return StructType(
        [
            StructField("capital", StringType(), False),
            StructField("dt", StringType(), True),
            StructField("id", LongType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True),
        ]
    )


@fixture
def expected_alert_data_schema():
    return StructType(
        [
            StructField("capital", StringType(), False),
            StructField("dt", StringType(), True),
            StructField("sender_name", StringType(), True),
            StructField("event", StringType(), True),
            StructField("start", StringType(), True),
            StructField("end", StringType(), True),
            StructField("description", StringType(), True),
            StructField("tags", StringType(), True),
        ]
    )


@fixture
def expected_climate_data():
    return [
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
            "capital": "Manaus",
        },
        {
            "dt": "2025-03-18 21:26:00",
            "sunrise": "2025-03-18 06:15:09",
            "sunset": "2025-03-18 18:23:56",
            "temp": 25.78,
            "feels_like": 25.9,
            "pressure": 1013,
            "humidity": 57,
            "dew_point": 16.62,
            "uvi": 0.0,
            "clouds": 0,
            "visibility": 10000,
            "wind_speed": 2.06,
            "wind_deg": 200,
            "wind_gust": 0.0,
            "rain_1h": 0.13,
            "snow_1h": 0.0,
            "capital": "Brasilia",
        },
    ]


@fixture
def expected_weather_data():
    return [
        {
            "dt": "2025-03-06 08:58:13",
            "id": 801,
            "main": "Clouds",
            "description": "algumas nuvens",
            "icon": "02d",
            "capital": "Manaus",
        },
        {
            "dt": "2025-03-18 21:26:00",
            "id": 500,
            "main": "Rain",
            "description": "chuva leve",
            "icon": "10n",
            "capital": "Brasilia",
        },
    ]


@fixture
def expected_alert_data():
    return [
        {
            "dt": "2025-03-06 08:58:13",
            "sender_name": "Instituto Nacional de Meteorologia",
            "event": "Chuvas Intensas",
            "start": "2025-03-05 09:45:00",
            "end": "2025-03-06 10:00:00",
            "description": "INMET publica aviso iniciando em: 05/03/2025 09:45. Chuva entre 30 e 60 mm/h ou 50 e 100 mm/dia, ventos intensos (60-100 km/h). Risco de corte de energia el\u00e9trica,  queda de galhos de \u00e1rvores, alagamentos e de descargas el\u00e9tricas.",
            "tags": "Rain Storm",
            "capital": "Manaus",
        },
        {
            "dt": "2025-03-06 08:58:13",
            "sender_name": "Instituto Nacional de Meteorologia",
            "event": "Chuvas Muito Intensas",
            "start": "2025-03-05 09:45:00",
            "end": "2025-03-06 10:00:00",
            "description": "INMET publica aviso iniciando em: 05/03/2025 09:45. Chuva entre 30 e 60 mm/h ou 50 e 100 mm/dia, ventos intensos (60-100 km/h). Risco de corte de energia el\u00e9trica,  queda de galhos de \u00e1rvores, alagamentos e de descargas el\u00e9tricas.",
            "tags": "Rain",
            "capital": "Manaus",
        },
        {
            "dt": "2025-03-18 21:26:00",
            "sender_name": "Instituto Nacional de Meteorologia",
            "event": "Baixa Umidade",
            "start": "2025-03-05 09:45:00",
            "end": "2025-03-06 10:00:00",
            "description": "Baixa umidade publicada pelo INMET",
            "tags": "Dry",
            "capital": "Brasilia",
        },
        {
            "dt": "2025-03-18 21:26:00",
            "sender_name": "Instituto Nacional de Meteorologia",
            "event": "Seca Extrema",
            "start": "2025-03-05 09:45:00",
            "end": "2025-03-06 10:00:00",
            "description": "Seca Extrema publicada pelo INMET",
            "tags": "Drought",
            "capital": "Brasilia",
        },
    ]
