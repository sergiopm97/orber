import os

from engines.etl_engine import ETLEngine
from utils.logger.configs import logging_configs
from utils.logger.logger import Logger


if __name__ == "__main__":

    logger_config = logging_configs["etl_engine"]

    logger = Logger(
        logger_config["logger_name"],
        logger_config["logging_format"],
        logger_config["logging_level"]
    )

    etl_logger = logger.create_logger()

    etl_logger.info(f"{logger.logger_name} starting")

    etl_engine = ETLEngine(
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster"),
        os.environ.get("mongodb_raw_collection")
    )

    etl_logger.info("Downloading soccer matches data")
    soccer_matches_data = etl_engine.get_data()
