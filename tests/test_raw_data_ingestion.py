import os
from pathlib import Path
import pytest
from dotenv import load_dotenv

from bronze.raw_data_ingestion import (
    get_api_url,
    request_data,
    save_data,
)

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
    return {"a": 1, "b": 2, "c": 3}


@pytest.fixture
def fake_data_stringfied():
    return '{"a": 1, "b": 2, "c": 3}'


@pytest.fixture
def test_dir(tmp_path):
    return Path(tmp_path, "bronze", "capital=Brasilia")


class TestDataIngestion:
    def test_get_api_url_raise(self, non_registered_state):
        with pytest.raises(KeyError):
            request_data(non_registered_state)

    def test_get_api_url_return(self, api_url_for_brasilia):
        assert api_url_for_brasilia == get_api_url("Brasilia")

    def test_save_data(
        self,
        fake_data,
        fake_data_stringfied,
        tmp_path: Path,
        test_dir: Path,
    ):

        save_data(fake_data, "Brasilia", tmp_path)
        assert os.path.exists(test_dir)

        dt_partition = os.listdir(test_dir)[-1]
        assert len(dt_partition) > 0 and ("date=" in dt_partition)

        file_path = os.listdir(test_dir.joinpath(dt_partition))[-1]
        with open(test_dir.joinpath(dt_partition, file_path), "r") as f:
            assert f.read() == fake_data_stringfied
