from typing import Callable, Dict, List

import numpy as np
import pandas as pd
from pymongo import MongoClient
from pymongo.results import InsertManyResult

from models.transformed_soccer_match import TransformedSoccerMatch


class ETLEngine:
    def __init__(
        self,
        origin_database_url: str,
        origin_database_cluster: str,
        origin_database_collection: str,
        target_database_url: str,
        target_database_cluster: str,
        target_database_collection: str
    ):
        self.origin_database_url = origin_database_url
        self.origin_database_cluster = origin_database_cluster
        self.origin_database_collection = origin_database_collection
        self.target_database_url = target_database_url
        self.target_database_cluster = target_database_cluster
        self.target_database_collection = target_database_collection

    def get_data(self) -> pd.DataFrame:
        """
        Download soccer matches
        data from origin database

        Returns:
            pd.DataFrame:
                DataFrame which contains dictionaries
                where each dictionary is one of the
                soccer matches with its data
        """

        mongodb_client = MongoClient(
            self.origin_database_url,
            tls=True,
            tlsAllowInvalidCertificates=True
        )

        raw_cluster = mongodb_client[self.origin_database_cluster]
        raw_data_collection = raw_cluster[self.origin_database_collection]

        return pd.DataFrame(
            [soccer_match for soccer_match in raw_data_collection.find({})]
        ).drop("_id", axis=1)

    @staticmethod
    def extract_match_winner(
        home_score: int,
        away_score: int
    ) -> int:
        """
        Obtain the winner of the soccer match comparing
        the goals scored by the home and the away teams

        Args:
            home_score (int):
                The number of goals
                scored by the home team

            away_score (int):
                The number of goals
                scored by the away team

        Returns:
            int:
                0 if the home team is the winner of the
                match and 1 if the match ended being
                a draw or a win for the away team
        """

        if home_score > away_score:
            return 0

        return 1

    @staticmethod
    def extract_match_goals(
        home_score: int,
        away_score: int
    ) -> int:
        """
        Obtain the total number of goals of a match
        adding the home goals and the away goals

        Args:
            home_score (int):
                The number of goals
                scored by the home team

            away_score (int):
                The number of goals
                scored by the away team

        Returns:
            int:
                Total number of goals that
                have been scored in the match
        """

        return home_score + away_score

    @staticmethod
    def extract_over_two_goals(
        home_score: int,
        away_score: int
    ) -> int:
        """
        Define if the total number of goals scored
        between the two teams is higher than 2 goals

        Args:
            home_score (int):
                The number of goals
                scored by the home team

            away_score (int):
                The number of goals
                scored by the away team

        Returns:
            int:
                0 if the total number of goals of
                the match is equal or less than
                2 and 1 if it is higher than 2
        """

        match_goals = home_score + away_score

        if match_goals <= 2:
            return 0

        return 1

    @staticmethod
    def extract_both_teams_scored(
        home_score: int,
        away_score: int
    ) -> int:
        """
        Define if each team of the soccer
        match have scored at least 1 goal

        Args:
            home_score (int):
                The number of goals
                scored by the home team

            away_score (int):
                The number of goals
                scored by the away team

        Returns:
            int:
                0 if one of the teams has not scored
                at least 1 goal and 1 if the two teams
                have scored at least 1 goal each
        """

        if home_score == 0 or away_score == 0:
            return 0

        return 1

    @staticmethod
    def generate_moving_average(
        soccer_matches: pd.DataFrame,
        date_column: str,
        reference_column: str,
        value_column: str,
        new_column: str
    ) -> pd.DataFrame:
        """
        Generate moving average of a numerical column
        taking in account a reference column and a date
        column where the window of the moving average is 6

        Args:
            soccer_matches (pd.DataFrame):
                DataFrame that contains all the soccer
                matches that are being processed in the ETL

            date_column (str):
                Value that defines the name of the column
                date within the soccer matches DataFrame

            reference_column (str):
                Column that is going to be the reference
                point to calculate the moving average

            value_column (str):
                Column to be used to extract
                the value of the moving average

        Returns:
            pd.DataFrame:
                Soccer matches DataFrame containing
                the calculated moving average column
        """

        matches_selected_columns = soccer_matches[
            [date_column, reference_column, value_column]
        ]

        mean_values = list()

        for soccer_match in matches_selected_columns.to_dict(orient="records"):

            reference_attr = soccer_match[reference_column]
            date_attr = soccer_match[date_column]

            filtered_soccer_matches = matches_selected_columns.loc[
                (soccer_matches[reference_column].values == reference_attr)
                & (soccer_matches[date_column].values < date_attr)
            ].iloc[-6:]

            if len(filtered_soccer_matches) < 6:
                mean_values.append(np.nan)

            else:
                mean_values.append(
                    np.round(np.mean(filtered_soccer_matches[value_column]), 2)
                )

        soccer_matches[new_column] = mean_values

        return soccer_matches.dropna()

    @staticmethod
    def sort_features_targets(
        soccer_matches: pd.DataFrame,
        features: List[str],
        targets: List[str],
    ) -> pd.DataFrame:
        """
        Sort the columns of the soccer matches
        DataFrame by features and targets

        Args:
            soccer_matches (pd.DataFrame):
                DataFrame containing all
                the soccer matches data

            features (List[str]):
                Original and generated features to
                be in the beginning of the DataFrame

            targets (List[str]):
                Original and generated targets to
                be in the end of the DataFrame

        Returns:
            pd.DataFrame:
                Soccer matches DataFrame sorted
                by the features and the targets
        """

        return soccer_matches[features + targets]

    @staticmethod
    def drop_useless_columns(
        soccer_matches: pd.DataFrame,
        columns: List[str]
    ) -> pd.DataFrame:
        """
        Drop columns that have been used
        in the ETL process but are no longer
        needed for anything else

        Args:
            soccer_matches (pd.DataFrame):
                DataFrame with the final version of
                the soccer matches data after the ETL

            columns (List[str]):
                Columns that are useless after the
                ETL process and need to be dropped

        Returns:
            pd.DataFrame:
                Final version of the DataFrame
                containing the soccer matches data
        """

        return soccer_matches.drop(columns, axis=1)

    @staticmethod
    def model_matches_data(
        soccer_matches: pd.DataFrame,
    ) -> List[TransformedSoccerMatch]:
        """
        Extract all soccer matches from the DataFrame and
        convert each of them to the TransformedSoccerMatch model

        Args:
            soccer_matches (pd.DataFrame):
                DataFrame with all the soccer matches
                data transformed after the ETL process

        Returns:
            List[TransformedSoccerMatch]:
                List of all transformed soccer matches data
                modeled by the TransformedSoccerMatch class
        """

        soccer_matches_dict = soccer_matches.to_dict(
            orient="records"
        )

        modeled_soccer_matches = list()

        for soccer_match in soccer_matches_dict:
            modeled_soccer_match = TransformedSoccerMatch(**soccer_match)
            modeled_soccer_matches.append(modeled_soccer_match)

        return modeled_soccer_matches

    def insert_transformed_matches(
        self,
        soccer_matches: List[TransformedSoccerMatch]
    ) -> Callable[[Dict[str, any]], InsertManyResult]:
        """
        Insert the transformed soccer
        matches data in the target database

        Args:
            soccer_matches (List[TransformedSoccerMatch]):
                Transformed soccer matches data that are
                ready to be called and inserted
                in the target database

        Returns:
            Callable[[Dict[str, any]], InsertManyResult]:
                Insertion of all matches data into the collection accepting
                the data in dictionary form and returning
                a default result from MongoDB
        """

        mongodb_client = MongoClient(
            self.target_database_url,
            tls=True,
            tlsAllowInvalidCertificates=True
        )

        transformed_cluster = mongodb_client[self.target_database_cluster]

        transformed_data_collection = transformed_cluster[
            self.target_database_collection
        ]

        if transformed_data_collection is not None:
            transformed_data_collection.drop()

        soccer_matches_dicts = [soccer_match.dict()
                                for soccer_match in soccer_matches]

        return transformed_data_collection.insert_many(soccer_matches_dicts)
