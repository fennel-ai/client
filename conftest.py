import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run_airbyte",
        action="store_true",
        default=False,
        help="run airbyte tests",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "airbyte: mark test that needs setup to " "run"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run_airbyte"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --run_airbyte option to run")
    for item in items:
        if "airbyte" in item.keywords:
            item.add_marker(skip_slow)
