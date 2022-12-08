import configparser

from engines.collector_engine import CollectorEngine

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"]
    )

    soccer_matches_df = collector_engine.get_data()
