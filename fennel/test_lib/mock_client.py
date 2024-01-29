from __future__ import annotations

import json
import logging
import os
import sys
import types
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union, Any

import pandas as pd

from fennel._vendor.requests import Response  # type: ignore
from fennel.client import Client
from fennel.datasets import Dataset, field, Pipeline, OnDemand  # noqa
from fennel.featuresets import Featureset, Feature, is_valid_feature
from fennel.gen.featureset_pb2 import (
    Extractor as ProtoExtractor,
)
from fennel.gen.schema_pb2 import DSSchema
from fennel.lib.graph_algorithms import (
    get_extractor_order,
)
from fennel.lib.includes import includes  # noqa
from fennel.lib.schema import get_datatype
from fennel.sources.sources import S3Connector
from fennel.test_lib.branch import Branch
from fennel.test_lib.integration_client import IntegrationClient
from fennel.test_lib.query_engine import QueryEngine
from fennel.test_lib.test_utils import cast_col_to_dtype

TEST_PORT = 50051
TEST_DATA_PORT = 50052
FENNEL_LOOKUP = "__fennel_lookup_exists__"
FENNEL_ORDER = "__fennel_order__"
FENNEL_TIMESTAMP = "__fennel_timestamp__"

logger = logging.getLogger(__name__)


class FakeResponse(Response):
    def __init__(self, status_code: int, content: str):
        self.status_code = status_code

        self.encoding = "utf-8"
        if status_code == 200:
            self._ok = True
            self._content = json.dumps({}).encode("utf-8")
            return
        self._content = json.dumps({"error": f"{content}"}, indent=2).encode(
            "utf-8"
        )


def get_extractor_func(extractor_proto: ProtoExtractor) -> Callable:
    fqn = f"{extractor_proto.feature_set_name}.{extractor_proto.name}"
    mod = types.ModuleType(fqn)
    code = (
        extractor_proto.pycode.imports + extractor_proto.pycode.generated_code
    )
    try:
        sys.modules[fqn] = mod
        exec(code, mod.__dict__)
    except Exception as e:
        raise Exception(
            f"Error while executing code for {fqn}:\n {code} \n: {str(e)}"
        )
    return mod.__dict__[extractor_proto.pycode.entry_point]


class MockClient(Client):
    def __init__(self):
        self.branches_map: Dict[str, Branch] = {}
        self.query_engine: QueryEngine = QueryEngine()

    # ----------------- Debug methods -----------------------------------------

    def get_dataset_df(
        self, dataset_name: str, branch: str = "main"
    ) -> pd.DataFrame:
        return self._get_branch(branch).get_dataset_df(dataset_name)

    # ----------------- Public methods -----------------------------------------

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        branch: str = "main",
        _batch_size: int = 1000,
    ):
        return self._get_branch(branch).log(webhook, endpoint, df, _batch_size)

    def sync(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        tier: Optional[str] = None,
        branch: str = "main",
    ):
        return self._get_branch(branch).sync(
            datasets, featuresets, preview, tier
        )

    def extract(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = "default",
        sampling_rate: Optional[float] = 1.0,
        branch: str = "main",
    ) -> pd.DataFrame:
        if log:
            raise NotImplementedError("log is not supported in MockClient")
        if input_dataframe.empty:
            return pd.DataFrame()
        branch_class = self._get_branch(branch)
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

    def extract_historical(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        timestamp_column: str,
        format: str = "pandas",
        input_dataframe: Optional[pd.DataFrame] = None,
        input_s3: Optional[S3Connector] = None,
        output_s3: Optional[S3Connector] = None,
        feature_to_column_map: Optional[Dict[Feature, str]] = None,
        branch: str = "main",
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
        branch_class = self._get_branch(branch)
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

    def extract_historical_progress(self, request_id):
        return FakeResponse(404, "Extract historical features not supported")

    def extract_historical_cancel_request(self, request_id):
        return FakeResponse(404, "Extract historical features not supported")

    def lookup(
        self,
        dataset_name: str,
        keys: List[Dict[str, Any]],
        fields: List[str],
        timestamps: List[Union[int, str, datetime]] = None,
    ):
        try:
            dataset = self.datasets[dataset_name]
            dataset_info = self.dataset_info[dataset_name]
        except KeyError:
            raise KeyError(f"Dataset: {dataset_name} not found")

        for field_name in fields:
            if field_name not in dataset_info.fields:
                raise ValueError(f"Field: {field_name} not in dataset")

        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl,
            self.data,
            self.aggregated_datasets,
            self.dataset_info,
            [dataset_name],
            None,
        )

        timestamps = (
            pd.Series(timestamps).apply(lambda x: self._parse_datetime(x))
            if timestamps
            else pd.Series([datetime.now() for _ in range(len(keys))])
        )

        keys_dict = defaultdict(list)
        for key in keys:
            for key_name in key.keys():
                keys_dict[key_name].append(key[key_name])

        data, found = dataset.lookup(
            timestamps,
            **{name: pd.Series(value) for name, value in keys_dict.items()},
        )

        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl,
            self.data,
            self.aggregated_datasets,
            self.dataset_info,
            None,
            None,
        )
        return data[fields].to_dict(orient="records"), found

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

    def _get_branch(self, branch: str) -> Branch:
        try:
            return self.branches_map[branch]
        except KeyError:
            raise KeyError(
                f"Branch: {branch} not found, please sync this branch and try again. "
                f"Available branches: {self.branches_map.keys()}"
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


def proto_to_dtype(proto_dtype) -> str:
    if proto_dtype.HasField("int_type"):
        return "int"
    elif proto_dtype.HasField("double_type"):
        return "float"
    elif proto_dtype.HasField("string_type"):
        return "string"
    elif proto_dtype.HasField("bool_type"):
        return "bool"
    elif proto_dtype.HasField("timestamp_type"):
        return "timestamp"
    elif proto_dtype.HasField("optional_type"):
        return f"optional({proto_to_dtype(proto_dtype.optional_type.of)})"
    else:
        return str(proto_dtype)


def cast_df_to_schema(
    df: pd.DataFrame, dsschema: DSSchema, pre_proc_cols: List[str] = []
) -> pd.DataFrame:
    # Handle fields in keys and values
    fields = list(dsschema.keys.fields) + list(dsschema.values.fields)
    df = df.copy()
    df = df.reset_index(drop=True)
    for f in fields:
        if f.name not in df.columns:
            if f.name in pre_proc_cols:
                continue
            raise ValueError(
                f"Field `{f.name}` not found in dataframe while logging to dataset"
            )
        try:
            series = cast_col_to_dtype(df[f.name], f.dtype)
            series.name = f.name
            df[f.name] = series
        except Exception as e:
            raise ValueError(
                f"Failed to cast data logged to column `{f.name}` of type `{proto_to_dtype(f.dtype)}`: {e}"
            )
    if dsschema.timestamp not in df.columns:
        if dsschema.timestamp in pre_proc_cols:
            return df
        raise ValueError(
            f"Timestamp column `{dsschema.timestamp}` not found in dataframe while logging to dataset"
        )
    try:
        df[dsschema.timestamp] = pd.to_datetime(df[dsschema.timestamp]).astype(
            "datetime64[ns]"
        )
    except Exception as e:
        raise ValueError(
            f"Failed to cast data logged to timestamp column {dsschema.timestamp}: {e}"
        )
    return df


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
            client = IntegrationClient()
            f = test_func(*args, **kwargs, client=client)
        return f

    return wrapper
