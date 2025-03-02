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


def get_data(state: str) -> tuple[bool, int, dict]:
    openweather_api_url = get_api_url(state)
    res = get(openweather_api_url)
    
    return res.ok, res.status_code, res.json()
        
        
def save_data(state: str, data: dict, root: str = ""):
    now = datetime.now().strftime("%Y%m%d_$H%M%S")
    
    save_path = f"{root}{os.sep}data{os.sep}weather{os.sep}{state}"
    if not os.path.exists(save_path):
        os.makedirs(save_path)
        
    file_name = f"{state}_weather_{now}.json"
    with open(f"{save_path}{os.sep}{file_name}", "w") as f:
        dump(data, f)
