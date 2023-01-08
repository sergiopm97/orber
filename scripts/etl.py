import ast
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
        os.environ.get("mongodb_raw_collection"),
        os.environ.get("mongodb_connection_2"),
        os.environ.get("mongodb_transformed_cluster"),
        os.environ.get("mongodb_transformed_collection"),
    )

    if args.mode == "transforming":
        etl_logger.info("Execution mode: transforming")

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

        etl_logger.info("Generating moving average for home team scored goals")
        soccer_matches_df = etl_engine.generate_moving_average(
            soccer_matches_df,
            config["DATE_COLUMN"]["name"],
            config["TEAMS"]["home_team"],
            config["SCORES"]["home_score"],
            config["NEW_FEATURES"]["home_scored_mean_6"]
        )

        etl_logger.info(
            "Generating moving average for home team conceded goals"
        )
        soccer_matches_df = etl_engine.generate_moving_average(
            soccer_matches_df,
            config["DATE_COLUMN"]["name"],
            config["TEAMS"]["home_team"],
            config["SCORES"]["away_score"],
            config["NEW_FEATURES"]["home_conceded_mean_6"]
        )

        etl_logger.info("Generating moving average for away team scored goals")
        soccer_matches_df = etl_engine.generate_moving_average(
            soccer_matches_df,
            config["DATE_COLUMN"]["name"],
            config["TEAMS"]["away_team"],
            config["SCORES"]["away_score"],
            config["NEW_FEATURES"]["away_scored_mean_6"]
        )

        etl_logger.info(
            "Generating moving average for away team conceded goals"
        )
        soccer_matches_df = etl_engine.generate_moving_average(
            soccer_matches_df,
            config["DATE_COLUMN"]["name"],
            config["TEAMS"]["away_team"],
            config["SCORES"]["home_score"],
            config["NEW_FEATURES"]["away_conceded_mean_6"]
        )

        final_features = ast.literal_eval(
            config["RAW_FEATURES_TARGETS"]["raw_features"]
        ) + \
            list(config["NEW_FEATURES"].values())

        final_targets = ast.literal_eval(
            config["RAW_FEATURES_TARGETS"]["raw_targets"]
        ) + \
            list(config["NEW_TARGETS"].values())

        etl_logger.info("Sorting the columns by features and targets")
        soccer_matches_df = etl_engine.sort_features_targets(
            soccer_matches_df,
            final_features,
            final_targets
        )

        useless_columns = ast.literal_eval(config["USELESS_COLUMNS"]["names"])

        etl_logger.info("Dropping useless columns after transforming")
        soccer_matches_df = etl_engine.drop_useless_columns(
            soccer_matches_df,
            useless_columns
        )

        etl_logger.info(f"Modelling {len(soccer_matches_df)} soccer matches")
        modeled_soccer_matches = etl_engine.model_matches_data(
            soccer_matches_df
        )

        etl_logger.info(
            f"Inserting {len(modeled_soccer_matches)} in target database"
        )
        etl_engine.insert_transformed_matches(modeled_soccer_matches)

    elif args.mode == "training":
        etl_logger.info("Execution mode: training")

    else:
        etl_logger.info("Execution mode: predicting")
