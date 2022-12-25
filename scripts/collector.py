import ast
import configparser
import logging
import os
import ssl
import sys

from dotenv import load_dotenv

from engines.collector_engine import CollectorEngine


if __name__ == "__main__":

    stream_handler = logging.StreamHandler(sys.stdout)

    logging_format = logging.Formatter(
        "[%(asctime)s] - %(levelname)s - %(message)s"
    )

    stream_handler.setFormatter(logging_format)

    logger = logging.getLogger(__name__)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    load_dotenv()

    logger.info("Starting collector engine")

    if sys.platform == "darwin":
        logger.info("Darwin platform detected -> HTTP context to be created")
        ssl._create_default_https_context = ssl._create_unverified_context

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster")
    )

    logger.info("Obtaining soccer matches data from source database")
    soccer_matches_df = collector_engine.get_data()

    logger.info("Obtaining target database")
    mongodb_client = collector_engine.get_target_database()

    logger.info(f"Filtering {len(soccer_matches_df)} soccer matches")
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

    logger.info(f"Modelling {len(filtered_soccer_matches_df)} soccer matches")
    modeled_soccer_matches = collector_engine.model_matches_data(
        filtered_soccer_matches_df,
        match_features + match_targets,
        config["DATE_COLUMN"]["name"]
    )

    logger.info("Checking if raw_data_collection already exists")
    if mongodb_client["raw_data_collection"] is not None:
        logger.info("Collection already exists -> Collection to be droped")
        mongodb_client["raw_data_collection"].drop()

    logger.info("Creating raw_data_collection in target database")
    raw_data_collection = mongodb_client["raw_data_collection"]

    logger.info(
        f"Inserting {len(modeled_soccer_matches)} "
        "soccer matches in target database"
    )
    for modeled_soccer_match in modeled_soccer_matches:
        collector_engine.move_soccer_match(
            raw_data_collection, modeled_soccer_match
        )

    logger.info("All matches inserted in target database successfully")
