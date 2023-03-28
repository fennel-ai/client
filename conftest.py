import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run_di",
        action="store_true",
        default=False,
        help="run airbyte tests",
    )
    parser.addoption(
        "--run_slow",
        action="store_true",
        default=False,
        help="run slow tests",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "airbyte: mark test that needs setup to " "run"
    )
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run_slow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if config.getoption("--run_di"):
        skip_di = pytest.mark.skip(reason="need --run_di option to run")
        for item in items:
            if "data_integration" in item.keywords:
                item.add_marker(skip_di)
