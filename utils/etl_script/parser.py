import argparse

from exceptions.etl_script import ModeNotSpecified, NotValidMode


def get_parser() -> argparse.ArgumentParser:
    """
    Define and create the parser as well
    as its arguments and parser conditions

    Returns:
        argparse.ArgumentParser:
            ETL run-based parser ready to
            collect arguments passed in execution
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

    return parser


def validate_args(
    args: argparse.Namespace
):
    """
    Validate that the arguments passed
    in the ETL execution are valid

    Args:
        args (argparse.Namespace):
            Arguments passed in
            the ETL execution

    Raises:
        ModeNotSpecified:
            Exception that raises when the
            ETL mode is not specified

        NotValidMode:
            Exception that raises when the
            specified ETL mode is not valid
    """

    if args.mode is None:
        raise ModeNotSpecified("ETL mode must be specified")

    elif args.mode not in ["transforming", "training", "predicting"]:
        raise NotValidMode(
            "Invalid mode given. Choose between "
            "transforming, training and predicting"
        )
