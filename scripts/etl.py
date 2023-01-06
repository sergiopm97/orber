import configparser
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

    config = configparser.ConfigParser()
    config.read("config/config.ini")

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

        etl_logger.info("Starting to transform the soccer matches data")

        winner_column = config["NEW_TARGETS"]["match_winner"]

        home_score_column = config["SCORES"]["home_score"]
        away_score_column = config["SCORES"]["away_score"]

        etl_logger.info("Creating the column with the winner of the match")
        soccer_matches_df[winner_column] = soccer_matches_df.apply(
            lambda x: etl_engine.extract_match_winner(
                x[home_score_column], x[away_score_column]), axis=1
        )

        goals_column = config["NEW_TARGETS"]["match_goals"]

        etl_logger.info("Creating the column with the goals of the match")
        soccer_matches_df[goals_column] = soccer_matches_df.apply(
            lambda x: etl_engine.extract_match_goals(
                x[home_score_column], x[away_score_column]), axis=1
        )

        over_2_goals_column = config["NEW_TARGETS"]["over_2_goals"]

        etl_logger.info("Creating the column with over 2 goals matches")
        soccer_matches_df[over_2_goals_column] = soccer_matches_df.apply(
            lambda x: etl_engine.extract_over_two_goals(
                x[home_score_column], x[away_score_column]), axis=1
        )

        both_teams_scored_column = config["NEW_TARGETS"]["both_teams_scored"]

        etl_logger.info("Creating the column with both teams scored matches")
        soccer_matches_df[both_teams_scored_column] = soccer_matches_df.apply(
            lambda x: etl_engine.extract_both_teams_scored(
                x[home_score_column], x[away_score_column]), axis=1
        )

        soccer_matches_df = etl_engine.generate_moving_average(
            soccer_matches_df,
            config["DATE_COLUMN"]["name"],
            config["TEAMS"]["home_team"],
            config["TEAMS"]["away_team"]
        )

    else:
        etl_logger.info("Execution mode: predicting")
