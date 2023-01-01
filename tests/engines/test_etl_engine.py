import configparser
import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from engines.etl_engine import ETLEngine


@pytest.fixture
def etl_engine() -> ETLEngine:
    load_dotenv()

    return ETLEngine(
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster"),
        os.environ.get("mongodb_raw_collection")
    )


@pytest.fixture
def cluster_soccer_matches_df(etl_engine: ETLEngine) -> pd.DataFrame:
    return etl_engine.get_data()


def test_cluster_not_empty(cluster_soccer_matches_df: pd.DataFrame):
    assert not cluster_soccer_matches_df.empty


def test_soccer_matches_winners(
    config: configparser.ConfigParser,
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    expected_winners = [0, 1, 0, 1, 1, 1, 0, 1, 0]
    expected_result = pd.DataFrame({"winner": expected_winners})

    home_score_column = config["SCORES"]["home_score"]
    away_score_column = config["SCORES"]["away_score"]

    generated_winners = soccer_matches_sample.apply(
        lambda x: etl_engine.extract_match_winner(
            x[home_score_column], x[away_score_column],
        ),
        axis=1
    )

    generated_result = pd.DataFrame({"winner": generated_winners})

    pd.testing.assert_frame_equal(expected_result, generated_result)


def test_soccer_matches_goals(
    config: configparser.ConfigParser,
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    expected_goals = [4.0, 4.0, 6.0, 2.0, 3.0, 0.0, 5.0, 4.0, 2.0]
    expected_result = pd.DataFrame({"goals": expected_goals})

    home_score_column = config["SCORES"]["home_score"]
    away_score_column = config["SCORES"]["away_score"]

    generated_goals = soccer_matches_sample.apply(
        lambda x: etl_engine.extract_match_goals(
            x[home_score_column], x[away_score_column],
        ),
        axis=1
    )

    generated_result = pd.DataFrame({"goals": generated_goals})

    pd.testing.assert_frame_equal(expected_result, generated_result)
