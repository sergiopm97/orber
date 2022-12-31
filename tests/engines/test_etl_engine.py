import os

import pandas as pd
import pytest
from dotenv import load_dotenv

from engines.etl_engine import ETLEngine


@pytest.fixture
def etl_engine() -> ETLEngine:
    load_dotenv()

    return ETLEngine(
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster"),
        os.environ.get("mongodb_raw_collection")
    )


@pytest.fixture
def cluster_soccer_matches_df(etl_engine: ETLEngine) -> pd.DataFrame:
    return etl_engine.get_data()


def test_cluster_not_empty(cluster_soccer_matches_df: pd.DataFrame):
    assert not cluster_soccer_matches_df.empty
