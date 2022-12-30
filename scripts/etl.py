import os

from engines.etl_engine import ETLEngine
from exceptions.etl_script import ModeNotSpecified, NotValidMode
from utils.etl_script.parser import get_args
from utils.logger.configs import logging_configs
from utils.logger.logger import Logger


if __name__ == "__main__":

    args = get_args()

    if args.mode is None:
        raise ModeNotSpecified("ETL mode must be specified")

    elif args.mode not in ["training", "predicting"]:
        raise NotValidMode(
            "Invalid mode given. Choose between training and predicting"
        )

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

    if args.mode == "training":
        etl_logger.info("Execution mode: training")

        etl_logger.info("Downloading soccer matches data")
        soccer_matches_df = etl_engine.get_data()

    else:
        etl_logger.info("Execution mode: predicting")
