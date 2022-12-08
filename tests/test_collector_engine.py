import configparser
from urllib.error import URLError

import pytest

from engines.collector_engine import CollectorEngine


@pytest.fixture
def config() -> configparser.ConfigParser():
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    return config


def test_get_data_ssl_error(config: configparser.ConfigParser):
    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"]
    )

    with pytest.raises(URLError):
        collector_engine.get_data()
