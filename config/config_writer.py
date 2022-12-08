import configparser


if __name__ == "__main__":

    config = configparser.ConfigParser()

    config["FIVETHIRTYEIGHT"] = {
        "database_url": "https://projects.fivethirtyeight.com/"
        "soccer-api/club/spi_matches.csv"
    }

    with open("config/config.ini", "w") as config_file:
        config.write(config_file)
