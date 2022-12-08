import configparser
import ssl

from engines.collector_engine import CollectorEngine

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    ssl._create_default_https_context = ssl._create_unverified_context

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"]
    )

    soccer_matches_df = collector_engine.get_data()
