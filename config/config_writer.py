import configparser


if __name__ == "__main__":

    config = configparser.ConfigParser()

    config["FIVETHIRTYEIGHT"] = {
        "database_url": "https://projects.fivethirtyeight.com/"
        "soccer-api/club/spi_matches.csv"
    }

    config["SEASON_FILTER"] = {
        "season_column": "season",
        "initial_season": 2017,
        "final_season": 2021
    }

    config["RAW_FEATURES_TARGETS"] = {
        "raw_features": [
            "date",
            "season",
            "league",
            "team1",
            "team2",
            "spi1",
            "spi2",
            "prob1",
            "probtie",
            "prob2",
            "proj_score1",
            "proj_score2",
        ],
        "raw_targets": ["score1", "score2"]
    }

    config["DATE_COLUMN"] = {
        "name": "date"
    }

    config["NEW_TARGETS"] = {
        "match_winner": "winner",
        "match_goals": "goals",
        "over_2_goals": "over2",
        "both_teams_scored": "btts"
    }

    config["SCORES"] = {
        "home_score": "score1",
        "away_score": "score2"
    }

    with open("config/config.ini", "w") as config_file:
        config.write(config_file)
