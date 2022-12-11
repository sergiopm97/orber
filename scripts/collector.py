import ast
import configparser
import os
import ssl
import sys

from dotenv import load_dotenv

from engines.collector_engine import CollectorEngine


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    load_dotenv()

    if sys.platform == "darwin":
        ssl._create_default_https_context = ssl._create_unverified_context

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster")
    )

    soccer_matches_df = collector_engine.get_data()

    mongodb_client = collector_engine.get_target_database()

    filtered_soccer_matches_df = collector_engine.filter_data(
        soccer_matches_df,
        config["SEASON_FILTER"]["season_column"],
        int(config["SEASON_FILTER"]["initial_season"]),
        int(config["SEASON_FILTER"]["final_season"])
    )

    match_features = ast.literal_eval(
        config["RAW_FEATURES_TARGETS"]["raw_features"]
    )

    match_targets = ast.literal_eval(
        config["RAW_FEATURES_TARGETS"]["raw_targets"]
    )

    modeled_soccer_matches = collector_engine.model_matches_data(
        filtered_soccer_matches_df,
        match_features + match_targets
    )
