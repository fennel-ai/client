from __future__ import annotations

import copy
import logging
import os
from datetime import datetime, timezone
from typing import (
    Dict,
    List,
    Optional,
    Union,
    Tuple,
    Set,
)

import pandas as pd

import fennel.gen.schema_pb2 as schema_proto
import fennel.lib.secrets
from fennel import connectors
from fennel.client import Client
from fennel.connectors.connectors import S3Connector
from fennel.datasets import Dataset, field, Pipeline, OnDemand  # noqa
from fennel.featuresets import (
    Featureset,
    Feature,
    is_valid_feature,
    is_valid_featureset,
)
from fennel.internal_lib.graph_algorithms import (
    get_extractor_order,
)
from fennel.internal_lib.schema import get_datatype
from fennel.internal_lib.to_proto import to_sync_request_proto
from fennel.internal_lib.utils import parse_datetime
from fennel.lib import includes  # noqa
from fennel.testing.branch import Branch
from fennel.testing.data_engine import (
    internal_webhook,
    gen_dataset_webhook_endpoint,
    ds_from_endpoint,
)
from fennel.testing.integration_client import IntegrationClient
from fennel.testing.query_engine import QueryEngine
from fennel.testing.test_utils import cast_col_to_arrow_dtype, FakeResponse

MAIN_BRANCH = "main"
MOCK_CLIENT_ATTR = "__mock_client__"

logger = logging.getLogger(__name__)


def log(dataset: Dataset, df: pd.DataFrame):
    if not hasattr(dataset, MOCK_CLIENT_ATTR):
        raise Exception(
            f"Dataset {dataset.__name__} is not registered with the client"
        )

    client = getattr(dataset, MOCK_CLIENT_ATTR)
    if dataset.num_pipelines() == 0:
        endpoint = gen_dataset_webhook_endpoint(dataset._name)
        return client.log(internal_webhook.name, endpoint, df)
    else:
        branch = client._branch
        return client.branches_map[branch].log_to_dataset(dataset, df)


class MockClient(Client):
    def __init__(
        self,
        branch: Optional[str] = None,
    ):
        if branch is None:
            branch = MAIN_BRANCH

        self.secrets: Dict[str, str] = dict()
        self._branch: str = branch
        self.branches_map: Dict[str, Branch] = {}
        self.query_engine: QueryEngine = QueryEngine()
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []

        # Adding branch
        self.branches_map[branch] = Branch(branch)
        if branch != MAIN_BRANCH:
            # We should always have a main branch
            self.branches_map[MAIN_BRANCH] = Branch(MAIN_BRANCH)

        # Monkey Patching the secrets
        fennel.lib.secrets.get = self._get_secret

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
            if webhook == internal_webhook.name:
                dataset_name = ds_from_endpoint(endpoint)
                raise Exception(
                    f"Dataset `{dataset_name}` not found. Ensure the dataset is registered."
                )
            raise Exception(
                f"Webhook endpoint {webhook}_{endpoint} not found in any branch"
            )

    def commit(
        self,
        message: str,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        env: Optional[str] = None,
        incremental: bool = False,
    ):
        def is_superset_featureset(featureset_new, featureset_old):
            features_new = set(
                [feature._name for feature in featureset_new._features]
            )
            features_old = set(
                [feature._name for feature in featureset_old._features]
            )
            return features_new.issuperset(features_old)

        def is_new_dataset_eligible(dataset_new, dataset_old):
            """
            This function check if dataset of same name comes in the increment mode,
            is the new dataset eligible or not. Eligible if:
            1. If version same and old == new then eligible.
            2. new != old then version should be higher.
            """
            if dataset_new.version > dataset_old.version:
                return True
            elif dataset_new.version < dataset_old.version:
                return False
            else:
                return dataset_old.signature() == dataset_new.signature()

        if incremental:
            cur_datasets = self.get_datasets()
            cur_featuresets = self.get_featuresets()
            # Union of current and new datasets
            if datasets is None:
                datasets = cur_datasets
            else:
                # Union both the datasets from current and new and for those
                # which are common, ensure the new dataset has a higher version
                for dataset in cur_datasets:
                    for new_dataset in datasets:
                        if dataset._name == new_dataset._name:
                            if not is_new_dataset_eligible(
                                new_dataset, dataset
                            ):
                                raise ValueError(
                                    f"Please update version of dataset: `{new_dataset._name}`"
                                )
                dataset_collection = set(datasets)
                dataset_names = set([dataset._name for dataset in datasets])
                for dataset in cur_datasets:
                    if dataset._name not in dataset_names:
                        dataset_collection.add(dataset)
                datasets = list(dataset_collection)

            # Union of current and new featuresets
            if featuresets is None:
                featuresets = cur_featuresets
            else:
                # Union both the featuresets from current and new and for those
                # which are common, ensure the new featureset is a superset of the
                # existing featureset
                for featureset in cur_featuresets:
                    for new_featureset in featuresets:
                        if featureset._name == new_featureset._name:
                            if not is_superset_featureset(
                                new_featureset, featureset
                            ):
                                raise Exception(
                                    f"Featureset {new_featureset._name} is not a superset of the existing featureset"
                                )
                featuresets_collection = set(featuresets)
                featureset_names = set(
                    [featureset._name for featureset in featuresets]
                )
                for featureset in cur_featuresets:
                    if featureset._name not in featureset_names:
                        featuresets_collection.add(featureset)
                featuresets = list(featuresets_collection)

        self.to_register_objects = []
        self.to_register = set()

        if datasets is not None:
            for dataset in datasets:
                if not isinstance(dataset, Dataset):
                    raise TypeError(
                        f"Expected a list of datasets, got `{dataset.__name__}`"
                        f" of type `{type(dataset)}` instead."
                    )
                setattr(dataset, MOCK_CLIENT_ATTR, self)
                self.add(dataset)
        if featuresets is not None:
            for featureset in featuresets:
                if not isinstance(featureset, Featureset):
                    raise TypeError(
                        f"Expected a list of featuresets, got `{featureset.__name__}`"
                        f" of type `{type(featureset)}` instead."
                    )
                setattr(featureset, MOCK_CLIENT_ATTR, self)
                self.add(featureset)
        # Run all validation for converting them to protos
        _ = to_sync_request_proto(self.to_register_objects, message, env)
        return self._get_branch().commit(
            datasets, featuresets, preview, incremental, env
        )

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
        outputs = self._resolve_output_features(branch_class, outputs)
        extractors_to_run = get_extractor_order(
            inputs, outputs, entities.extractors
        )
        timestamps = cast_col_to_arrow_dtype(
            pd.Series([datetime.now(timezone.utc)] * len(input_dataframe)),
            schema_proto.DataType(timestamp_type=schema_proto.TimestampType()),
        )
        return self.query_engine.run_extractors(
            extractors_to_run,
            data_engine,
            entities,
            input_dataframe,
            outputs,
            timestamps,
            False,
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
        timestamps = timestamps.apply(lambda x: parse_datetime(x))
        input_feature_names = self._get_feature_name_from_inputs(inputs)
        input_dataframe = self._transform_input_dataframe_from_inputs(
            input_dataframe, inputs, input_feature_names
        )
        outputs = self._resolve_output_features(branch_class, outputs)
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
            True,
        )
        assert output_df.shape[0] == len(timestamps), (
            f"Output dataframe has {output_df.shape[0]} rows, but there are only {len(timestamps)} "
            "timestamps"
        )
        output_df[timestamp_column] = timestamps
        return output_df

    def track_offline_query(self, request_id):
        return FakeResponse(
            404, "Offline Query features not supported in MockClient"
        )

    def cancel_offline_query(self, request_id):
        return FakeResponse(
            404, "Offline Query features not supported in MockClient"
        )

    def lookup(
        self,
        dataset: Union[str, Dataset],
        keys: pd.DataFrame,
        fields: Optional[List[str]] = None,
        timestamps: Optional[pd.Series] = None,
    ) -> Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        dataset_name = dataset if isinstance(dataset, str) else dataset._name
        branch_class = self._get_branch()
        data_engine = branch_class.get_data_engine()
        return self.query_engine.lookup(
            data_engine, dataset_name, keys, fields, timestamps
        )

    def inspect(
        self,
        dataset: Union[str, Dataset],
        n: int = 10,
    ) -> pd.DataFrame:
        dataset_name = dataset if isinstance(dataset, str) else dataset._name
        branch_class = self._get_branch()
        df = branch_class.get_dataset_df(dataset_name)
        if df.shape[0] <= n:
            return df
        return df.sample(n)

    def erase(
        self, dataset_name: Union[str, Dataset], erase_keys: pd.DataFrame
    ):
        branch_class = self._get_branch()
        data_engine = branch_class.get_data_engine()
        if isinstance(dataset_name, Dataset):
            dataset_name = dataset_name.__name__
        elif isinstance(dataset_name, str):
            dataset_name = dataset_name
        else:
            raise TypeError(
                f"Expected a list of datasets, got `{dataset_name.__name__}`"
                f" of type `{type(dataset_name)}` instead."
            )
        return data_engine.erase(
            dataset_name, erase_keys.to_dict(orient="records")
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
            raise Exception(f"Branch `{name}` already exists")

        if name == from_branch:
            raise Exception(
                "New Branch name cannot be same as the branch that needs to be cloned"
            )

        if from_branch not in self.branches_map:
            raise Exception(f"Branch `{from_branch}` does not exist")
        self.branches_map[name] = copy.deepcopy(self.branches_map[from_branch])
        self.branches_map[name].name = name
        self.checkout(name)
        return FakeResponse(200, "Ok")

    def delete_branch(self, name: str):
        if name not in self.branches_map:
            raise Exception(f"Branch name: {name} does not exist")
        if name == MAIN_BRANCH:
            raise Exception("Cannot delete the main branch.")
        cur_branch = self._branch
        del self.branches_map[name]
        if cur_branch == name:
            print("Deleting current branch, checking out to main branch")
            self.checkout(MAIN_BRANCH)
        else:
            self.checkout(cur_branch)
        return FakeResponse(200, "Ok")

    def list_branches(self) -> List[str]:
        return list(self.branches_map.keys())

    def checkout(self, name: str):
        self._branch = name

    # ----------------------- Secret API's -----------------------------------

    def get_secret(self, secret_name: str) -> Optional[str]:
        return self._get_secret(secret_name)

    def add_secret(self, secret_name: str, secret_value: str):
        self.secrets[secret_name] = secret_value
        return FakeResponse(200, "Ok")

    def update_secret(self, secret_name: str, secret_value: str) -> str:
        self.secrets[secret_name] = secret_value
        return FakeResponse(200, "Ok")

    def delete_secret(self, secret_name: str) -> str:
        try:
            del self.secrets[secret_name]
            return FakeResponse(200, "Ok")
        except KeyError:
            raise KeyError(f"Secret : `{secret_name}` does not exist.")

    # --------------- Public MockClient Specific methods -------------------

    def sleep(self, seconds: float = 0):
        pass

    def integration_mode(self):
        return "mock"

    def is_integration_client(self) -> bool:
        return False

    # ----------------- Private methods --------------------------------------

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
            else:
                raise Exception(
                    f"Please provide a valid input, got : {input_feature}."
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
            input_dataframe[input_col] = cast_col_to_arrow_dtype(
                input_dataframe[input_col], col_type
            )
        return input_dataframe

    def _get_secret(self, secret_name: str) -> Optional[str]:
        try:
            return self.secrets[secret_name]
        except KeyError:
            raise KeyError(f"Secret : `{secret_name}` does not exist.")

    @staticmethod
    def _resolve_output_features(
        branch_class: Branch, outputs: List[Union[Feature, Featureset, str]]
    ) -> List[Union[Feature, Featureset, str]]:
        output_feature_names: List[Union[Feature, Featureset, str]] = []
        for output in outputs:
            if isinstance(output, Feature):
                output_feature_names.append(output)
            elif isinstance(output, Featureset):
                output_feature_names.append(output)
            elif isinstance(output, str):
                if is_valid_feature(output):
                    output_feature_names.append(output)
                elif is_valid_featureset(output):
                    output_feature_names.extend(
                        branch_class.get_features_from_fs(output)
                    )
                else:
                    raise Exception(
                        f"Please provide a valis string for outputs, got : `{output}`."
                    )
            else:
                raise Exception(f"Invalid type of outputs : {type(output)}.")
        return output_feature_names


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
