import configparser
import os
import ssl
import sys

from dotenv import load_dotenv

from engines.collector_engine import CollectorEngine


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    load_dotenv()

    if sys.platform == "darwin":
        ssl._create_default_https_context = ssl._create_unverified_context

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"],
        os.environ.get("mongodb_connection"),
        os.environ.get("mongodb_raw_cluster")
    )

    soccer_matches_df = collector_engine.get_data()

    mongodb_client = collector_engine.get_target_database()
