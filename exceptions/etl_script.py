class ModeNotSpecified(Exception):
    """
    Rises when the mode argument is
    not passed to the ETL execution
    """

    pass


class NotValidMode(Exception):
    """
    Rises when the mode provided for ETL
    execution is neither training nor predicting
    """

    pass
