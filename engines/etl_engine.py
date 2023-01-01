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
