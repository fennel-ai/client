from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import Dict, List, Tuple, Union, Optional

import numpy as np
import pandas as pd
from requests import Response

import fennel.datasets.datasets
from fennel.client import Client
from fennel.datasets import Dataset, Pipeline, OnDemand
from fennel.featuresets import Featureset, Feature, Extractor
from fennel.gen.dataset_pb2 import CreateDatasetRequest
from fennel.lib.graph_algorithms import (
    get_extractor_order,
    is_extractor_graph_cyclic,
)
from fennel.lib.schema import schema_check
from fennel.test_lib.executor import Executor

TEST_PORT = 50051
TEST_DATA_PORT = 50052
FENNEL_LOOKUP = "__fennel_lookup_exists__"
FENNEL_ORDER = "__fennel_order__"


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


def dataset_lookup_impl(
    data: Dict[str, pd.DataFrame],
    datasets: Dict[str, _DatasetInfo],
    allowed_datasets: Optional[List[str]],
    cls_name: str,
    ts: pd.Series,
    properties: List[str],
    keys: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    if cls_name not in datasets:
        raise ValueError(
            f"Dataset {cls_name} not found, please ensure it is synced."
        )
    if allowed_datasets is not None and cls_name not in allowed_datasets:
        raise ValueError(
            f"Extractor is not allowed to access dataset {cls_name}, enabled "
            f"datasets are {allowed_datasets}"
        )
    right_key_fields = datasets[cls_name].key_fields
    if len(right_key_fields) == 0:
        raise ValueError(
            f"Dataset {cls_name} does not have any key fields. "
            f"Cannot perform lookup operation on it."
        )
    if len(right_key_fields) != len(keys.columns):
        raise ValueError(
            f"Dataset {cls_name} has {len(right_key_fields)} key fields, "
            f"but {len(keys.columns)} key fields were provided."
        )
    if cls_name not in data:
        timestamp_col = datasets[cls_name].timestamp_field
        # Create a dataframe with all nulls
        val_cols = datasets[cls_name].fields
        if len(properties) > 0:
            val_cols = [
                x for x in val_cols if x in properties or x in keys.columns
            ]
        val_cols.remove(timestamp_col)
        empty_df = pd.DataFrame(
            columns=val_cols, data=[[None] * len(val_cols)] * len(keys)
        )
        return empty_df, pd.Series(np.array([False] * len(keys)))
    right_df = data[cls_name]
    timestamp_field = datasets[cls_name].timestamp_field
    join_columns = keys.columns.tolist()
    timestamp_length = len(ts)
    if timestamp_length != keys.shape[0]:
        raise ValueError(
            f"Timestamp length {timestamp_length} does not match key length "
            f"{keys.shape[0]} for dataset {cls_name}."
        )
    keys[timestamp_field] = ts
    keys[FENNEL_ORDER] = np.arange(len(keys))

    # Sort the keys by timestamp
    keys = keys.sort_values(timestamp_field)
    right_df[FENNEL_LOOKUP] = True
    df = pd.merge_asof(
        left=keys,
        right=right_df,
        on=timestamp_field,
        by=join_columns,
        direction="backward",
    )
    df = df.set_index(FENNEL_ORDER).loc[np.arange(len(df)), :]
    keys = keys.drop(columns=[FENNEL_ORDER])
    found = df[FENNEL_LOOKUP].apply(lambda x: x is not np.nan)
    # Check if an on_demand is found
    if datasets[cls_name].on_demand:
        on_demand_keys = keys[~found].reset_index(drop=True)
        args = [
            on_demand_keys[col]
            for col in keys.columns
            if col != timestamp_field
        ]
        on_demand_df, on_demand_found = datasets[cls_name].on_demand.func(
            on_demand_keys[timestamp_field], *args
        )
        # Filter out the columns that are not in the dataset
        df = df[found]
        df = pd.concat([df, on_demand_df], ignore_index=True, axis=0)
        found = pd.concat([found, on_demand_found])
    # drop the timestamp column
    df = df.drop(columns=[FENNEL_LOOKUP])
    if len(properties) > 0:
        df = df[properties]
    df = df.reset_index(drop=True)
    return df, found


@dataclass
class _DatasetInfo:
    fields: List[str]
    key_fields: List[str]
    timestamp_field: str
    on_demand: OnDemand


class MockClient(Client):
    def __init__(self):
        super().__init__(url=f"localhost:{TEST_PORT}")
        self.dataset_requests: Dict[str, CreateDatasetRequest] = {}
        self.datasets: Dict[str, _DatasetInfo] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl, self.data, self.datasets, None
        )
        self.extractors: List[Extractor] = []

    # ----------------- Public methods -----------------

    def is_integration_client(self) -> bool:
        return False

    def log(self, dataset_name: str, df: pd.DataFrame):
        if dataset_name not in self.dataset_requests:
            return FakeResponse(404, f"Dataset {dataset_name} not found")
        dataset_req = self.dataset_requests[dataset_name]
        timestamp_field = self.datasets[dataset_name].timestamp_field
        if timestamp_field not in df.columns:
            return FakeResponse(
                400,
                f"Timestamp field {timestamp_field} not found in dataframe",
            )
        if str(df[timestamp_field].dtype) != "datetime64[ns]":
            return FakeResponse(
                400,
                f"Timestamp field {timestamp_field} is not of type "
                f"datetime64[ns] but found {df[timestamp_field].dtype} in "
                f"dataset {dataset_name}",
            )
        # Check if the dataframe has the same schema as the dataset
        schema = {}
        for field in dataset_req.fields:
            schema[field.name] = field.dtype
        exceptions = schema_check(schema, df)
        if len(exceptions) > 0:
            return FakeResponse(400, str(exceptions))
        self._merge_df(df, dataset_name)

        for pipeline in self.listeners[dataset_name]:
            executor = Executor(self.data)
            ret = executor.execute(pipeline)
            if ret is None:
                continue
            # Recursively log the output of the pipeline to the datasets
            resp = self.log(pipeline.dataset_name, ret.df)
            if resp.status_code != 200:
                return resp
        return FakeResponse(200, "OK")

    def sync(
        self, datasets: List[Dataset] = [], featuresets: List[Featureset] = []
    ):
        self._reset()
        for dataset in datasets:
            self.dataset_requests[
                dataset._name
            ] = dataset.create_dataset_request_proto()
            self.datasets[dataset._name] = _DatasetInfo(
                dataset.fields(),
                dataset.key_fields,
                dataset.timestamp_field,
                dataset.on_demand,
            )
            for pipeline in dataset._pipelines:
                for input in pipeline.inputs:
                    self.listeners[input._name].append(pipeline)

        for featureset in featuresets:
            # Check if the dataset used by the extractor is registered
            for extractor in featureset.extractors:
                datasets = [
                    x.name for x in extractor.get_dataset_dependencies()
                ]
                for dataset in datasets:
                    if dataset not in self.dataset_requests:
                        raise ValueError(
                            f"Dataset {dataset} not found in sync call"
                        )
            self.extractors.extend(featureset.extractors)

        if is_extractor_graph_cyclic(self.extractors):
            raise Exception("Cyclic graph detected in extractors")

        return FakeResponse(200, "OK")

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if input_df.empty:
            return pd.DataFrame()
        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn)
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn for f in input_feature.features]
                )
        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_df.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_df.columns}"
            )
        extractors = get_extractor_order(
            input_feature_list, output_feature_list, self.extractors
        )
        return self._run_extractors(extractors, input_df, output_feature_list)

    # ----------------- Private methods -----------------

    def _prepare_extractor_args(
        self, extractor: Extractor, intermediate_data: Dict[str, pd.Series]
    ):
        args = []
        for input in extractor.inputs:
            if isinstance(input, Feature):
                if input.fqn in intermediate_data:
                    args.append(intermediate_data[input.fqn])
                else:
                    raise Exception(
                        f"Feature {input} could not be "
                        f"calculated by any extractor."
                    )
            elif isinstance(input, Featureset):
                series = []
                for feature in input.features:
                    if feature.fqn in intermediate_data:
                        series.append(intermediate_data[feature.fqn])
                    else:
                        raise Exception(
                            f"Feature {feature} couldn't be "
                            f"calculated by any extractor."
                        )
                args.append(pd.concat(series, axis=1))
            else:
                raise Exception(
                    f"Unknown input type {type(input)} found "
                    f"during feature extraction."
                )
        return args

    def _run_extractors(
        self,
        extractors: List[Extractor],
        input_df: pd.DataFrame,
        output_feature_list: List[Union[Feature, Featureset]],
    ):
        timestamps = pd.Series([pd.Timestamp.now()] * len(input_df))
        # Map of feature name to the pandas series
        intermediate_data: Dict[str, pd.Series] = {}
        for col in input_df.columns:
            intermediate_data[col] = input_df[col]
        for extractor in extractors:
            prepare_args = self._prepare_extractor_args(
                extractor, intermediate_data
            )
            allowed_datasets = [
                x.name for x in extractor.get_dataset_dependencies()
            ]
            print(
                f"Running extractor {extractor} on datasets "
                f"{allowed_datasets}"
            )
            fennel.datasets.datasets.dataset_lookup = partial(
                dataset_lookup_impl, self.data, self.datasets, allowed_datasets
            )
            output = extractor.func(timestamps, *prepare_args)
            if isinstance(output, pd.Series):
                intermediate_data[output.name] = output
            elif isinstance(output, pd.DataFrame):
                for col in output.columns:
                    intermediate_data[col] = output[col]
            else:
                raise Exception(
                    f"Extractor {extractor.name} returned "
                    f"invalid type {type(output)}"
                )

        # Prepare the output dataframe
        output_df = pd.DataFrame()
        for feature in output_feature_list:
            if isinstance(feature, Feature):
                output_df[feature.fqn] = intermediate_data[feature.fqn]
            elif isinstance(feature, Featureset):
                for f in feature.features:
                    output_df[f.fqn] = intermediate_data[f.fqn]
            else:
                raise Exception(
                    f"Unknown feature type {type(feature)} found "
                    f"during feature extraction."
                )
        return output_df

    def _merge_df(self, df: pd.DataFrame, dataset_name: str):
        # Filter the dataframe to only include the columns in the schema
        columns = self.datasets[dataset_name].fields
        input_columns = df.columns.tolist()
        # Check that input columns are a subset of the dataset columns
        if not set(columns).issubset(set(input_columns)):
            raise ValueError(
                f"Dataset columns {columns} are not a subset of "
                f"Input columns {input_columns}"
            )
        df = df[columns]
        if dataset_name not in self.data:
            self.data[dataset_name] = df
        else:
            self.data[dataset_name] = pd.concat([self.data[dataset_name], df])
        # Sort by timestamp
        timestamp_field = self.datasets[dataset_name].timestamp_field
        self.data[dataset_name] = self.data[dataset_name].sort_values(
            timestamp_field
        )

    def _reset(self):
        self.dataset_requests: Dict[str, CreateDatasetRequest] = {}
        self.datasets: Dict[str, _DatasetInfo] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl, self.data, self.datasets, None
        )
        self.extractors: List[Extractor] = []


def mock_client(test_func):
    def wrapper(*args, **kwargs):
        client = MockClient()
        f = test_func(*args, **kwargs, client=client)
        # if (
        #         "USE_INT_CLIENT" in os.environ
        #         and int(os.environ.get("USE_INT_CLIENT")) == 1
        # ):
        #     print("Running rust client tests")
        #     client = IntegrationClient()
        #     f = test_func(*args, **kwargs, client=client)
        return f

    return wrapper
