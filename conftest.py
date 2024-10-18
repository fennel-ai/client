import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--run_di",
        action="store_true",
        default=False,
        help="run data integration tests",
    )
    parser.addoption(
        "--run_slow",
        action="store_true",
        default=False,
        help="run slow tests",
    )
    parser.addoption(
        "--unit",
        action="store_true",
        default=False,
        help="run only unit tests",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "di: mark test that needs setup to run")
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "integration: mark test as integration")
    config.addinivalue_line("markers", "unit: mark test as a unit test")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--unit"):
        skip_integration = pytest.mark.skip(reason="skipping integration tests")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
    else:
        for item in items:
            if "unit" not in item.keywords:
                item.add_marker(pytest.mark.integration)

    if not config.getoption("--run_slow"):
        skip_slow = pytest.mark.skip(reason="need --run_slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--run_di"):
        skip_di = pytest.mark.skip(reason="need --run_di option to run")
        for item in items:
            if "data_integration" in item.keywords:
                item.add_marker(skip_di)
