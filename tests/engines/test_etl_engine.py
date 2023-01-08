import configparser
import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from engines.etl_engine import ETLEngine
from models.transformed_soccer_match import TransformedSoccerMatch


@pytest.fixture
def etl_engine() -> ETLEngine:
    load_dotenv()

    return ETLEngine(
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster"),
        os.environ.get("mongodb_raw_collection"),
        os.environ.get("mongodb_connection_2"),
        os.environ.get("mongodb_transformed_cluster"),
        os.environ.get("mongodb_transformed_collection"),
    )


@pytest.fixture
def cluster_soccer_matches_df(etl_engine: ETLEngine) -> pd.DataFrame:
    return etl_engine.get_data()


@pytest.fixture
def soccer_matches_sample_moving_avg() -> pd.DataFrame:
    return pd.read_csv("tests/samples/soccer_matches_sample_moving_avg.csv")


@pytest.fixture
def transformed_soccer_matches_sample() -> pd.DataFrame:
    return pd.read_csv("tests/samples/transformed_soccer_matches_sample.csv")


def test_cluster_not_empty(cluster_soccer_matches_df: pd.DataFrame):
    assert not cluster_soccer_matches_df.empty


def test_soccer_matches_winners(
    config: configparser.ConfigParser,
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    expected_winners = [0, 1, 0, 1, 1, 1, 0, 1, 0]
    expected_result = pd.DataFrame(
        {config["NEW_TARGETS"]["match_winner"]: expected_winners}
    )

    home_score_column = config["SCORES"]["home_score"]
    away_score_column = config["SCORES"]["away_score"]

    generated_winners = soccer_matches_sample.apply(
        lambda x: etl_engine.extract_match_winner(
            x[home_score_column], x[away_score_column],
        ),
        axis=1
    )

    generated_result = pd.DataFrame(
        {config["NEW_TARGETS"]["match_winner"]: generated_winners}
    )

    pd.testing.assert_frame_equal(expected_result, generated_result)


def test_soccer_matches_goals(
    config: configparser.ConfigParser,
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    expected_goals = [4.0, 4.0, 6.0, 2.0, 3.0, 0.0, 5.0, 4.0, 2.0]
    expected_result = pd.DataFrame(
        {config["NEW_TARGETS"]["match_goals"]: expected_goals}
    )

    home_score_column = config["SCORES"]["home_score"]
    away_score_column = config["SCORES"]["away_score"]

    generated_goals = soccer_matches_sample.apply(
        lambda x: etl_engine.extract_match_goals(
            x[home_score_column], x[away_score_column],
        ),
        axis=1
    )

    generated_result = pd.DataFrame(
        {config["NEW_TARGETS"]["match_goals"]: generated_goals}
    )

    pd.testing.assert_frame_equal(expected_result, generated_result)


def test_soccer_over_2_matches(
    config: configparser.ConfigParser,
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    expected_over_2_matches = [1, 1, 1, 0, 1, 0, 1, 1, 0]
    expected_result = pd.DataFrame(
        {
            config["NEW_TARGETS"]["over_2_goals"]: expected_over_2_matches
        }
    )

    home_score_column = config["SCORES"]["home_score"]
    away_score_column = config["SCORES"]["away_score"]

    generated_over_2_matches = soccer_matches_sample.apply(
        lambda x: etl_engine.extract_over_two_goals(
            x[home_score_column], x[away_score_column],
        ),
        axis=1
    )

    generated_result = pd.DataFrame(
        {config["NEW_TARGETS"]["over_2_goals"]: generated_over_2_matches}
    )

    pd.testing.assert_frame_equal(expected_result, generated_result)


def test_soccer_both_teams_scored_matches(
    config: configparser.ConfigParser,
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    expected_both_teams_scored_matches = [1, 1, 1, 0, 0, 0, 1, 1, 0]
    expected_result = pd.DataFrame(
        {
            config["NEW_TARGETS"]["both_teams_scored"]:
            expected_both_teams_scored_matches
        }
    )

    home_score_column = config["SCORES"]["home_score"]
    away_score_column = config["SCORES"]["away_score"]

    generated_both_teams_scored_matches = soccer_matches_sample.apply(
        lambda x: etl_engine.extract_both_teams_scored(
            x[home_score_column], x[away_score_column],
        ),
        axis=1
    )

    generated_result = pd.DataFrame(
        {
            config["NEW_TARGETS"]["both_teams_scored"]:
            generated_both_teams_scored_matches
        }
    )

    pd.testing.assert_frame_equal(expected_result, generated_result)


def test_moving_average(
        config: configparser.ConfigParser,
        etl_engine: ETLEngine,
        soccer_matches_sample_moving_avg: pd.DataFrame,
):
    expected_moving_average = [1.33, 1.17, 1.17, 0.83, 0.83, 1.00]

    expected_result = pd.DataFrame(
        {config["NEW_FEATURES"]["home_scored_mean_6"]: expected_moving_average}
    )

    generated_result = etl_engine.generate_moving_average(
        soccer_matches_sample_moving_avg,
        config["DATE_COLUMN"]["name"],
        config["TEAMS"]["home_team"],
        config["SCORES"]["home_score"],
        config["NEW_FEATURES"]["home_scored_mean_6"]
    )[[config["NEW_FEATURES"]["home_scored_mean_6"]]].reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_result, generated_result)


def test_sort_columns(
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    soccer_matches_columns = soccer_matches_sample.columns.to_list()

    fake_features = soccer_matches_columns[0:2]
    fake_targets = soccer_matches_columns[4:6]

    sorted_soccer_matches = etl_engine.sort_features_targets(
        soccer_matches_sample, fake_features, fake_targets
    )

    soccer_sorted_matches_columns = sorted_soccer_matches.columns.to_list()

    assert (fake_features + fake_targets) == soccer_sorted_matches_columns


def test_drop_useless_columns(
    etl_engine: ETLEngine,
    soccer_matches_sample: pd.DataFrame
):
    useless_columns = soccer_matches_sample.columns.to_list()[0:4]

    soccer_matches_final_columns = etl_engine.drop_useless_columns(
        soccer_matches_sample, useless_columns
    )

    final_columns = soccer_matches_final_columns.columns.to_list()

    assert useless_columns not in final_columns


def test_model_transformed_matches_data_types(
    etl_engine: ETLEngine,
    transformed_soccer_matches_sample: pd.DataFrame
):
    modeled_soccer_matches = etl_engine.model_matches_data(
        transformed_soccer_matches_sample
    )

    for modeled_soccer_match in modeled_soccer_matches:
        assert isinstance(modeled_soccer_match, TransformedSoccerMatch)
