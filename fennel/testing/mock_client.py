from __future__ import annotations

import copy
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple

import pandas as pd

from fennel._vendor.requests import Response  # type: ignore
from fennel.client import Client
from fennel.datasets import Dataset, field, Pipeline, OnDemand  # noqa
from fennel.featuresets import Featureset, Feature, is_valid_feature
from fennel.internal_lib.graph_algorithms import (
    get_extractor_order,
)
from fennel.internal_lib.schema import get_datatype
from fennel.lib import includes  # noqa
from fennel.sources.sources import S3Connector
from fennel.testing.branch import Branch
from fennel.testing.integration_client import IntegrationClient
from fennel.testing.query_engine import QueryEngine
from fennel.testing.test_utils import cast_col_to_dtype, FakeResponse

MAIN_BRANCH = "main"

logger = logging.getLogger(__name__)


class MockClient(Client):
    def __init__(self, branch: Optional[str] = None):
        if branch is None:
            branch = MAIN_BRANCH

        self._branch: str = branch
        self.branches_map: Dict[str, Branch] = {}
        self.query_engine: QueryEngine = QueryEngine()

        # Adding branch
        self.branches_map[branch] = Branch(branch)

    # ----------------- Debug methods -----------------------------------------

    def get_dataset_df(self, dataset_name: str) -> pd.DataFrame:
        return self._get_branch().get_dataset_df(dataset_name)

    def get_datasets(self) -> List[Dataset]:
        """
        Return list of datasets in the branch
        Returns:
            List[Dataset]
        """
        return self._get_branch().get_datasets()

    def get_featuresets(self) -> List[Featureset]:
        """
        Return list of datasets in the branch
        Returns:
            List[Dataset]
        """
        return self._get_branch().get_featuresets()

    # ----------------- Public methods -----------------------------------------

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        _batch_size: int = 1000,
    ):
        if df.shape[0] == 0:
            return FakeResponse(200, "OK")

        at_least_one_ok = False
        for branch in self.branches_map:
            response = self.branches_map[branch].log(
                webhook, endpoint, df, _batch_size
            )
            if response.status_code == 200:
                at_least_one_ok = True
        if at_least_one_ok:
            return FakeResponse(200, "OK")
        else:
            FakeResponse(
                404,
                f"Webhook endpoint {webhook}_{endpoint} not "
                f"found in any branch",
            )

    def commit(
        self,
        message: str,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        tier: Optional[str] = None,
    ):
        return self._get_branch().commit(datasets, featuresets, preview, tier)

    def query(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = "default",
        sampling_rate: Optional[float] = 1.0,
    ) -> pd.DataFrame:
        if log:
            raise NotImplementedError("log is not supported in MockClient")
        if input_dataframe.empty:
            return pd.DataFrame()
        branch_class = self._get_branch()
        entities = branch_class.get_entities()
        data_engine = branch_class.get_data_engine()
        input_feature_names = self._get_feature_name_from_inputs(inputs)
        input_dataframe = self._transform_input_dataframe_from_inputs(
            input_dataframe, inputs, input_feature_names
        )
        extractors_to_run = get_extractor_order(
            inputs, outputs, entities.extractors
        )
        timestamps = pd.Series([datetime.utcnow()] * len(input_dataframe))
        return self.query_engine.run_extractors(
            extractors_to_run,
            data_engine,
            entities,
            input_dataframe,
            outputs,
            timestamps,
        )

    def query_offline(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        timestamp_column: str,
        format: str = "pandas",
        input_dataframe: Optional[pd.DataFrame] = None,
        input_s3: Optional[S3Connector] = None,
        output_s3: Optional[S3Connector] = None,
        feature_to_column_map: Optional[Dict[Feature, str]] = None,
    ) -> Union[pd.DataFrame, pd.Series]:
        if format != "pandas":
            raise NotImplementedError(
                "Only pandas format is supported in MockClient"
            )
        if input_dataframe is None:
            raise ValueError(
                "input must contain a key 'input_dataframe' with the input dataframe"
            )
        assert feature_to_column_map is None, "column_mapping is not supported"
        assert input_s3 is None, "input_s3 is not supported"
        assert output_s3 is None, "output_s3 is not supported"

        if input_dataframe.empty:
            return pd.DataFrame()
        branch_class = self._get_branch()
        entities = branch_class.get_entities()
        data_engine = branch_class.get_data_engine()
        timestamps = input_dataframe[timestamp_column]
        timestamps = pd.to_datetime(timestamps)
        input_feature_names = self._get_feature_name_from_inputs(inputs)
        input_dataframe = self._transform_input_dataframe_from_inputs(
            input_dataframe, inputs, input_feature_names
        )
        extractors_to_run = get_extractor_order(
            inputs, outputs, entities.extractors
        )
        output_df = self.query_engine.run_extractors(
            extractors_to_run,
            data_engine,
            entities,
            input_dataframe,
            outputs,
            timestamps,
        )
        assert output_df.shape[0] == len(timestamps), (
            f"Output dataframe has {output_df.shape[0]} rows, but there are only {len(timestamps)} "
            "timestamps"
        )
        output_df[timestamp_column] = timestamps
        return output_df

    def track_offline_query(self, request_id):
        return FakeResponse(404, "Offline Query features not supported")

    def cancel_offline_query(self, request_id):
        return FakeResponse(404, "Offline Query features not supported")

    def lookup(
        self,
        dataset_name: str,
        keys: pd.DataFrame,
        fields: Optional[List[str]] = None,
        timestamps: Optional[pd.Series] = None,
    ) -> Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        branch_class = self._get_branch()
        data_engine = branch_class.get_data_engine()
        return self.query_engine.lookup(
            data_engine, dataset_name, keys, fields, timestamps
        )

    def inspect(
        self,
        dataset_name: str,
        n: int = 10,
    ) -> List[Dict[str, Any]]:
        branch_class = self._get_branch()
        return (
            branch_class.get_dataset_df(dataset_name)
            .last(n)
            .to_dict(orient="records")
        )

    # ----------------------- Branch API's -----------------------------------

    def init_branch(self, name: str):
        if name in self.branches_map:
            raise ValueError(f"Branch name: `{name}` already exists")
        self.branches_map[name] = Branch(name)
        self.checkout(name)
        return FakeResponse(200, "Ok")

    def clone_branch(self, name: str, from_branch: str):
        if name in self.branches_map:
            return FakeResponse(400, f"Branch name: {name} already exists")
        if name == from_branch:
            return FakeResponse(
                400,
                "New Branch name cannot be same as the branch that needs to be cloned",
            )
        if from_branch not in self.branches_map:
            return FakeResponse(
                400, f"Branch name: {from_branch} does not exist"
            )
        self.branches_map[name] = copy.deepcopy(self.branches_map[from_branch])
        self.branches_map[name].name = name
        self.checkout(name)
        return FakeResponse(200, "Ok")

    def delete_branch(self, name: str):
        if name not in self.branches_map:
            return FakeResponse(400, f"Branch name: {name} does not exist")
        if name == MAIN_BRANCH:
            return FakeResponse(400, "Cannot delete main branch")
        del self.branches_map[name]
        self._branch = MAIN_BRANCH
        return FakeResponse(200, "Ok")

    def list_branches(self) -> List[str]:
        return list(self.branches_map.keys())

    def checkout(self, name: str):
        self._branch = name

    # --------------- Public MockClient Specific methods -------------------

    def sleep(self, seconds: float = 0):
        pass

    def integration_mode(self):
        return "mock"

    def is_integration_client(self) -> bool:
        return False

    # ----------------- Private methods --------------------------------------
    def _parse_datetime(self, value: Union[int, str, datetime]) -> datetime:
        if isinstance(value, int):
            try:
                return pd.to_datetime(value, unit="s")
            except ValueError:
                try:
                    return pd.to_datetime(value, unit="ms")
                except ValueError:
                    return pd.to_datetime(value, unit="us")
        if isinstance(value, str):
            return pd.to_datetime(value)
        return value

    def _get_branch(self) -> Branch:
        try:
            return self.branches_map[self._branch]
        except KeyError:
            raise KeyError(
                f"Branch: `{self._branch}` not found, please sync this branch and try again. "
                f"Available branches: {str(list(self.branches_map.keys()))}"
            )

    def _get_feature_name_from_inputs(
        self, inputs: List[Union[Feature, str]]
    ) -> List[str]:
        input_feature_names = []
        for input_feature in inputs:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn_)
            elif isinstance(input_feature, str) and is_valid_feature(
                input_feature
            ):
                input_feature_names.append(input_feature)
            elif isinstance(input_feature, Featureset):
                raise Exception(
                    "Providing a featureset as input is deprecated. "
                    f"List the features instead. {[f.fqn() for f in input_feature.features]}."
                )
        return input_feature_names

    def _transform_input_dataframe_from_inputs(
        self,
        input_dataframe: pd.DataFrame,
        inputs: List[Union[Feature, str]],
        input_feature_names: List[str],
    ) -> pd.DataFrame:
        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )
        for input_col, feature in zip(input_dataframe.columns, inputs):
            if isinstance(feature, str):
                continue
            col_type = get_datatype(feature.dtype)  # type: ignore
            input_dataframe[input_col] = cast_col_to_dtype(
                input_dataframe[input_col], col_type
            )
        return input_dataframe

    def _reset(self):
        self.branches_map: Dict[str, Branch] = {}


def mock(test_func):
    def wrapper(*args, **kwargs):
        f = True
        if "data_integration" not in test_func.__name__:
            client = MockClient()
            f = test_func(*args, **kwargs, client=client)
        if (
            "USE_INT_CLIENT" in os.environ
            and int(os.environ.get("USE_INT_CLIENT")) == 1
        ):
            mode = os.environ.get("FENNEL_TEST_MODE", "inmemory")
            print("Running rust client tests in mode:", mode)
            client = IntegrationClient(mode)
            f = test_func(*args, **kwargs, client=client)
        return f

    return wrapper
