import logging
import sys


class Logger:
    def __init__(
        self,
        logger_name: str,
        logging_format: str,
        logging_level: int
    ) -> None:
        self.logger_name = logger_name
        self.logging_format = logging_format
        self.logging_level = logging_level

    def create_logger(self) -> logging.Logger:
        """
        Initialize a new logger
        from the given attributes

        Returns:
            logging.Logger:
                Logger created from
                the given attributes
        """

        logger = logging.getLogger(__name__)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(self.logging_format)

        logger.addHandler(stream_handler)
        logger.setLevel(self.logging_level)

        return logger
