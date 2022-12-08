from engines.collector_engine import CollectorEngine
import configparser


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config/config.ini")

    collector_engine = CollectorEngine(
        config["FIVETHIRTYEIGHT"]["database_url"]
    )

    print()
