import configparser

import pandas as pd
import pytest


@pytest.fixture
def config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    return config


@pytest.fixture
def soccer_matches_sample() -> pd.DataFrame:
    return pd.read_csv("tests/samples/soccer_matches_sample.csv")
