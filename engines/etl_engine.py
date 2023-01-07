import numpy as np
import pandas as pd
from pymongo import MongoClient


class ETLEngine:
    def __init__(
        self,
        target_database_url: str,
        target_database_cluster: str,
        target_database_collection: str
    ):
        self.target_database_url = target_database_url
        self.target_database_cluster = target_database_cluster
        self.target_database_collection = target_database_collection

    def get_data(self) -> pd.DataFrame:
        """
        Download soccer matches
        data from target database

        Returns:
            pd.DataFrame:
                DataFrame which contains dictionaries
                where each dictionary is one of the
                soccer matches with its data
        """

        mongodb_client = MongoClient(
            self.target_database_url,
            tls=True,
            tlsAllowInvalidCertificates=True
        )

        raw_cluster = mongodb_client[self.target_database_cluster]
        raw_data_collection = raw_cluster[self.target_database_collection]

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
        value_column: str
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

        soccer_matches[reference_column + "_" + value_column
                       + "_" + "mean_last_6"] = mean_values

        return soccer_matches.dropna()
