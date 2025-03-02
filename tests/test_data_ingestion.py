import os
import pytest
from data_ingestion import get_api_url, save_data
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
key = os.getenv("API_KEY")

@pytest.fixture
def non_registered_state():
    return "Texas"

@pytest.fixture
def api_url_for_brasilia():
    base_url = "https://api.openweathermap.org/data/3.0/onecall?"
    params = f"lat=-15.7934036&lon=-47.8823172&exclude=hourly,daily,minutely&appid={key}&units=metric&lang=pt_br"
    return base_url + params

@pytest.fixture
def fake_data():
    return {
        "a": 1,
        "b": 2,
        "c": 3
    }
    
@pytest.fixture
def fake_data_stringfied():
    return '{"a": 1, "b": 2, "c": 3}'

@pytest.fixture
def test_dir(tmp_path):
    return f"{tmp_path}{os.sep}data{os.sep}weather{os.sep}test_state"

@pytest.fixture
def test_filename():
    now = datetime.now().strftime("%Y%m%d_$H%M%S")
    return f"test_state_weather_{now}.json"

class TestDataIngestion:
    def test_get_api_url_raise(self, non_registered_state):
        with pytest.raises(KeyError):
            get_api_url(non_registered_state)
        
    def test_get_api_url_return(self, api_url_for_brasilia):
        assert api_url_for_brasilia == get_api_url("brasilia")

    def test_save_data(self, test_dir, test_filename, fake_data, fake_data_stringfied, tmp_path):        
        save_data("test_state", fake_data, tmp_path)
        
        assert os.path.exists(test_dir)
        with open(test_dir + os.sep + test_filename, "r") as f:
            assert f.read() == fake_data_stringfied
        
        