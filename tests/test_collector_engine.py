import configparser
import os
import sys
from urllib.error import URLError
from dotenv import load_dotenv
from pymongo import MongoClient

import pytest

from engines.collector_engine import CollectorEngine


@pytest.fixture
def config() -> configparser.ConfigParser():
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    return config


@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="The error is only raised on Mac OS X machines"
)
def test_get_data_ssl_error(config: configparser.ConfigParser):
    fake_target_database_url = str()
    fake_target_database_cluster = str()

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        fake_target_database_url,
        fake_target_database_cluster
    )

    with pytest.raises(URLError):
        collector_engine.get_data()


def test_get_target_database(config: configparser.ConfigParser):
    load_dotenv()

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster")
    )

    mongodb_client = collector_engine.get_target_database()

    assert isinstance(mongodb_client.client, MongoClient)
