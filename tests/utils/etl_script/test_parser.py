from argparse import ArgumentParser, Namespace

import pytest

from exceptions.etl_script import ModeNotSpecified, NotValidMode
from utils.etl_script.parser import get_parser, validate_args


@pytest.fixture
def parser() -> Namespace:
    return get_parser()


def test_mode_not_given(parser: ArgumentParser):
    test_args = []

    with pytest.raises(ModeNotSpecified):
        args = parser.parse_args(test_args)
        validate_args(args)


def test_invalid_mode(parser: ArgumentParser):
    test_args = ["--mode", "invalid_mode"]

    with pytest.raises(NotValidMode):
        args = parser.parse_args(test_args)
        validate_args(args)
