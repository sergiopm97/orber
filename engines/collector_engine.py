import pandas as pd
from pymongo import MongoClient


class CollectorEngine:
    def __init__(
        self,
        origin_database_url: str,
        target_database_url: str,
        target_database_cluster: str
    ) -> None:
        self.origin_database_url = origin_database_url
        self.target_database_url = target_database_url
        self.target_database_cluster = target_database_cluster

    def get_data(self) -> pd.DataFrame:
        """
        Obtain historical soccer match data
        from FiveThirtyEight's origin database


        Returns:
            pd.DataFrame:
                DataFrame with historical
                data on soccer matches
        """

        return pd.read_csv(self.origin_database_url)

    def get_target_database(self) -> MongoClient:
        """
        Generate MongoDB client for the target database
        and for the target cluster of the collector

        Returns:
            MongoClient:
                MongoDB client of the target database and
                for the target cluster of the collector
        """

        mongodb_client = MongoClient(self.target_database_url)
        return mongodb_client[self.target_database_cluster]
