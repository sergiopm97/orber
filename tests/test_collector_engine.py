import ast
import configparser
import os
import sys
from urllib.error import URLError

import pandas as pd
import pytest
from dotenv import load_dotenv
from pymongo import MongoClient

from engines.collector_engine import CollectorEngine
from models.raw_soccer_match import RawSoccerMatch


@pytest.fixture
def config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read("config/config.ini")
    return config


@pytest.fixture
def collector_engine(
    config: configparser.ConfigParser
) -> CollectorEngine:
    load_dotenv()

    return CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster")
    )


@pytest.fixture
def soccer_matches_sample() -> pd.DataFrame:
    return pd.read_csv("tests/samples/soccer_matches_sample.csv")


@pytest.fixture
def modeled_soccer_matches(
    config: configparser.ConfigParser,
    soccer_matches_sample: pd.DataFrame,
    collector_engine: CollectorEngine
) -> pd.DataFrame:
    match_features = ast.literal_eval(
        config["RAW_FEATURES_TARGETS"]["raw_features"]
    )

    match_targets = ast.literal_eval(
        config["RAW_FEATURES_TARGETS"]["raw_targets"]
    )

    return collector_engine.model_matches_data(
        soccer_matches_sample,
        match_features + match_targets,
        config["DATE_COLUMN"]["name"]
    )


@pytest.mark.skipif(
    sys.platform != "darwin",
    reason="The error is only raised on Mac OS X machines"
)
def test_get_data_ssl_error(collector_engine: CollectorEngine):
    with pytest.raises(URLError):
        collector_engine.get_data()


def test_get_target_database(collector_engine: CollectorEngine):
    mongodb_client = collector_engine.get_target_database()
    assert isinstance(mongodb_client.client, MongoClient)


def test_filter_data(
    config: configparser.ConfigParser,
    collector_engine: CollectorEngine,
    soccer_matches_sample: pd.DataFrame
):
    initial_season_test = 2018
    final_season_test = 2020

    season_range_test = [2018, 2019, 2020]

    filtered_soccer_matches = collector_engine.filter_data(
        soccer_matches_sample,
        config["SEASON_FILTER"]["season_column"],
        initial_season_test,
        final_season_test
    )

    unique_season_values = filtered_soccer_matches[
        config["SEASON_FILTER"]["season_column"]
    ].value_counts().sort_values(ascending=False)

    assert unique_season_values.index.to_list() == season_range_test


def test_model_matches_data_no_nan(
    modeled_soccer_matches: pd.DataFrame
):
    modeled_soccer_matches_df = pd.DataFrame(
        [modeled_soccer_match.dict()
         for modeled_soccer_match in modeled_soccer_matches
         ]
    )

    assert modeled_soccer_matches_df.isna().sum().sum() == 0


def test_model_matches_data_types(
    modeled_soccer_matches: pd.DataFrame
):
    for modeled_soccer_match in modeled_soccer_matches:
        assert isinstance(modeled_soccer_match, RawSoccerMatch)
