import os
from pathlib import Path
from requests import get
from dotenv import load_dotenv
from datetime import datetime
from json import dump

from bronze.capital_coords import capital_coords

load_dotenv()


def get_api_url(capital: str):
    if capital not in capital_coords:
        raise KeyError("Coordinates for given capital not available.")

    lat = capital_coords[capital]["latitude"]
    long = capital_coords[capital]["longitude"]
    api_key = os.getenv("API_KEY")

    base_url = "https://api.openweathermap.org/data/3.0/onecall?"
    url = (
        base_url
        + f"lat={lat}&lon={long}&exclude=hourly,daily,minutely&appid={api_key}&units=metric&lang=pt_br"
    )

    return url


def request_data(capital: str):
    url = get_api_url(capital)
    res = get(url)

    return {"status_code": res.status_code, "data": res.json()}


def save_data(data: dict, capital: str, root: str | Path):
    now = datetime.now()
    save_path = Path(root, "bronze", f"capital={capital}", f"date={now.date()}")

    save_path.mkdir(parents=True, exist_ok=True)

    dt = now.isoformat(sep="T", timespec="seconds").replace(":", "-")
    file_name = f"{capital}_{dt}.json"
    with open(save_path.joinpath(file_name), "w") as f:
        dump(data, f)
