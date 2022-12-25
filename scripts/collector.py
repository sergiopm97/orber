import ast
import configparser
import os
import ssl
import sys

from dotenv import load_dotenv

from engines.collector_engine import CollectorEngine
from utils.logger.configs import logging_configs
from utils.logger.logger import Logger


if __name__ == "__main__":

    logger_config = logging_configs["collector_engine"]

    logger = Logger(
        logger_config["logger_name"],
        logger_config["logging_format"],
        logger_config["logging_level"]
    )

    collector_logger = logger.create_logger()

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    load_dotenv()

    collector_logger.info(f"{logger.logger_name} starting")

    if sys.platform == "darwin":
        collector_logger.info(
            "Darwin platform detected -> HTTP context to be created"
        )
        ssl._create_default_https_context = ssl._create_unverified_context

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster")
    )

    collector_logger.info("Obtaining soccer matches data from source database")
    soccer_matches_df = collector_engine.get_data()

    collector_logger.info("Obtaining target database")
    mongodb_client = collector_engine.get_target_database()

    collector_logger.info(f"Filtering {len(soccer_matches_df)} soccer matches")
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

    collector_logger.info(
        f"Modelling {len(filtered_soccer_matches_df)} soccer matches"
    )
    modeled_soccer_matches = collector_engine.model_matches_data(
        filtered_soccer_matches_df,
        match_features + match_targets,
        config["DATE_COLUMN"]["name"]
    )

    raw_data_collection_name = os.environ.get("mongodb_raw_collection")

    collector_logger.info("Checking if raw_data_collection already exists")
    if mongodb_client[raw_data_collection_name] is not None:
        collector_logger.info(
            "Collection already exists -> Collection to be droped"
        )
        mongodb_client[raw_data_collection_name].drop()

    collector_logger.info("Creating raw_data_collection in target database")
    raw_data_collection = mongodb_client[raw_data_collection_name]

    collector_logger.info(
        f"Inserting {len(modeled_soccer_matches)} "
        "soccer matches in target database"
    )
    for modeled_soccer_match in modeled_soccer_matches:
        collector_engine.move_soccer_match(
            raw_data_collection, modeled_soccer_match
        )

    collector_logger.info(
        "All matches inserted in target database successfully"
    )
