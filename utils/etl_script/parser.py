import argparse


def get_args() -> argparse.Namespace:
    """
    Define the parser and collect the
    arguments provided in the ETL run

    Returns:
        argparse.Namespace:
            Namespace which contains the
            arguments passed in ETL execution
    """

    parser = argparse.ArgumentParser(
        prog="ETL",
        description="Execute the ETL in training or predicting mode"
    )

    parser.add_argument(
        "--mode",
        type=str,
        help="Select the ETL execution mode",
    )

    return parser.parse_args()
