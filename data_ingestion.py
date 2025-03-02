import os
from requests import get
from dotenv import load_dotenv
from datetime import datetime
from json import dump
from state_coords import state_coords

load_dotenv()
api_key = os.getenv("API_KEY")

def get_api_url(state: str) -> str:
    if state not in state_coords:
        raise KeyError("State coordinates not available.")
    
    lat, long = state_coords[state]["latitude"], state_coords[state]["longitude"]
    base_url = "https://api.openweathermap.org/data/3.0/onecall?"
    url = base_url + f"lat={lat}&lon={long}&exclude=hourly,daily,minutely&appid={api_key}&units=metric&lang=pt_br"

    return url

def get_data(state: str):
    openweather_api_url = get_api_url(state)
    res = get(openweather_api_url)

    if res.ok:
        now = datetime.now()
        data = res.json()
        
        if not os.path.exists(f"data/weather/{state}"):
            os.mkdir(f"data/weather/{state}")
            
        file_name = f"{state}_weather_{now}.json"
        with open(f"data/weather/{state}/{file_name}", "w") as f:
            dump(data, f)
    else:
        print(res.status_code)
        print(res.json())
        
get_data("brasilia")