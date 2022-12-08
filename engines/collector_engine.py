import pandas as pd


class CollectorEngine:
    def __init__(self, origin_database_url: str) -> None:
        self.origin_database_url = origin_database_url

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
