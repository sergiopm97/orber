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
