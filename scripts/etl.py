import os

from engines.etl_engine import ETLEngine
from utils.etl_script.parser import get_parser, validate_args
from utils.logger.configs import logging_configs
from utils.logger.logger import Logger


if __name__ == "__main__":

    parser = get_parser()

    args = parser.parse_args()
    validate_args(args)

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
