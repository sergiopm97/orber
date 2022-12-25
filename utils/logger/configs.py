import logging

logging_configs = {
    "collector_engine": {
        "logger_name": "Collector engine",
        "logging_format": logging.Formatter(
            "[%(asctime)s] - %(levelname)s - %(message)s"
        ),
        "logging_level": logging.INFO,
    }
}
