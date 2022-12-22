from typing import Callable, Dict, List

import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.results import InsertOneResult

from models.raw_soccer_match import RawSoccerMatch


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

    def filter_data(
        self,
        soccer_matches: pd.DataFrame,
        season_column: str,
        initial_season: int,
        final_season: int,
    ) -> pd.DataFrame:
        """
        Select data sample based on a filter which takes
        into account the season of the soccer match

        Returns:
            pd.DataFrame:
                DataFrame with soccer matches
                filtered by the specified seasons
        """

        return soccer_matches[
            (soccer_matches[season_column] >= initial_season) &
            (soccer_matches[season_column] <= final_season)
        ]

    def get_target_database(self) -> MongoClient:
        """
        Generate MongoDB client for the target database
        and for the target cluster of the collector

        Returns:
            MongoClient:
                MongoDB client of the target database and
                for the target cluster of the collector
        """

        mongodb_client = MongoClient(
            self.target_database_url,
            tls=True,
            tlsAllowInvalidCertificates=True
        )

        return mongodb_client[self.target_database_cluster]

    @staticmethod
    def model_matches_data(
        soccer_matches: pd.DataFrame,
        features_targets: list,
    ) -> List[RawSoccerMatch]:
        """
        Extract all soccer matches from the DataFrame and
        convert each of them to the RawSoccerMatch model

        Args:
            soccer_matches (pd.DataFrame):
                DataFrame with all historical
                soccer match data in raw format

            features_targets (list):
                Selected input and predicted columns
                to be saved in the target database

        Returns:
            List[RawSoccerMatch]:
                List of all historical soccer match
                data modeled by the RawSoccerMatch class
        """

        matches_features_targets = soccer_matches[features_targets].dropna()

        matches_features_targes_dict = matches_features_targets.to_dict(
            orient="records"
        )

        modeled_soccer_matches = list()

        for soccer_match in matches_features_targes_dict:
            modeled_soccer_match = RawSoccerMatch(**soccer_match)
            modeled_soccer_matches.append(modeled_soccer_match)

        return modeled_soccer_matches

    def move_soccer_match(
        self,
        mongodb_collection: Collection,
        soccer_match: RawSoccerMatch
    ) -> Callable[[Dict[str, any]], InsertOneResult]:
        """
        Insert data from a soccer
        match into MongoDB collection

        Args:
            collection (MongoClient):
                MongoDB collection in which
                the match is to be saved

            soccer_match (RawSoccerMatch):
                Data of the soccer match to be
                inserted in the provided collection

        Returns:
            Callable[[Dict[str, any]], InsertOneResult]:
                Insertion of party data into the collection accepting
                the data in dictionary form and returning
                a default result from MongoDB
        """

        return mongodb_collection.insert_one(soccer_match.dict())
