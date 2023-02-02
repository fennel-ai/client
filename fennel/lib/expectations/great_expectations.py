from dataclasses import dataclass
from typing import cast, Callable, Optional

from great_expectations.data_context import DataContext  # type: ignore
from great_expectations.data_context.types.base import (  # type: ignore
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

import great_expectations as ge

GE_ATTR_FUNC = "__fennel_great_expectations_func__"
FENNEL_GE_SUITE = "fennel_expectation_suite"


@dataclass
class Expectations:
    version: int
    func: Callable
    context: DataContext
    json_config: Optional[str] = None


def expectations(func: Optional[Callable] = None, version: int = 0):
    """
    expectations is a decorator for a function that defines Great
    Expectations on a dataset or featureset.
    """

    def _create_expectations(c: Callable, version: int):
        if not callable(c):
            raise TypeError("expectations can only be applied to functions")
        data_context_config = DataContextConfig(
            config_version=2,
            plugins_directory=None,
            config_variables_file_path=None,
            store_backend_defaults=InMemoryStoreBackendDefaults(),
        )
        datasource_config = {
            "name": "fennel_placeholder_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    "batch_identifiers": ["default_identifier_name"],
                },
            },
        }

        context = ge.get_context(project_config=data_context_config)
        context.add_datasource(**datasource_config)
        setattr(c, GE_ATTR_FUNC, Expectations(version, c, context))
        return c

    def wrap(c: Callable):
        return _create_expectations(c, version)

    if func is None:
        # We're being called as @expectations(version=int).
        if version is None:
            raise TypeError("version must be specified as an integer.")
        if not isinstance(version, int):
            raise TypeError("version for expectations must be an int.")
        return wrap

    func = cast(Callable, func)
    # @expectations decorator was used without arguments
    return wrap(func)
