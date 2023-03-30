from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import Dict, List, Tuple, Union, Optional

import numpy as np
import pandas as pd
from requests import Response

import fennel.datasets.datasets
from fennel.client import Client
from fennel.datasets import Dataset, Pipeline, OnDemand
from fennel.featuresets import Featureset, Feature, Extractor
from fennel.gen.dataset_pb2 import CoreDataset
from fennel.gen.featureset_pb2 import CoreFeatureset, Feature as ProtoFeature
from fennel.gen.schema_pb2 import Field, DSSchema, Schema
from fennel.lib.graph_algorithms import (
    get_extractor_order,
    is_extractor_graph_cyclic,
)
from fennel.lib.schema import data_schema_check
from fennel.lib.to_proto import (
    dataset_to_proto,
    featureset_to_proto,
    features_from_fs,
)
from fennel.test_lib.executor import Executor
from fennel.test_lib.integration_client import IntegrationClient

TEST_PORT = 50051
TEST_DATA_PORT = 50052
FENNEL_LOOKUP = "__fennel_lookup_exists__"
FENNEL_ORDER = "__fennel_order__"
FENNEL_TIMESTAMP = "__fennel_timestamp__"


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
    extractor_name: Optional[str],
    cls_name: str,
    ts: pd.Series,
    fields: List[str],
    keys: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    if cls_name not in datasets:
        raise ValueError(
            f"Dataset {cls_name} not found, please ensure it is synced."
        )
    if allowed_datasets is not None and cls_name not in allowed_datasets:
        raise ValueError(
            f"Extractor `{extractor_name}` is not allowed to access dataset "
            f"`{cls_name}`, enabled datasets are {allowed_datasets}. "
            f"Use `depends_on` param in @extractor to specify dataset "
            f"dependencies."
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
        # Create a dataframe with all nulls
        val_cols = datasets[cls_name].fields
        if len(fields) > 0:
            val_cols = [x for x in val_cols if x in fields or x in keys.columns]
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
    right_df[FENNEL_TIMESTAMP] = right_df[timestamp_field]
    df = pd.merge_asof(
        left=keys,
        right=right_df,
        on=timestamp_field,
        by=join_columns,
        direction="backward",
        suffixes=("", "_right"),
    )
    df.drop(timestamp_field, axis=1, inplace=True)
    df.rename(columns={FENNEL_TIMESTAMP: timestamp_field}, inplace=True)
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
        on_demand_df, on_demand_found = datasets[cls_name].on_demand.bound_func(
            on_demand_keys[timestamp_field], *args
        )
        # Filter out the columns that are not in the dataset
        df = df[found]
        df = pd.concat([df, on_demand_df], ignore_index=True, axis=0)
        found = pd.concat([found, on_demand_found])
    df.drop(columns=[FENNEL_LOOKUP], inplace=True)
    right_df.drop(columns=[FENNEL_LOOKUP], inplace=True)
    if len(fields) > 0:
        df = df[fields]
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
        self.dataset_requests: Dict[str, CoreDataset] = {}
        self.featureset_requests: Dict[str, CoreFeatureset]
        self.features_for_fs: Dict[str, List[ProtoFeature]]
        self.datasets: Dict[str, _DatasetInfo] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl, self.data, self.datasets, None, None
        )
        self.extractors: List[Extractor] = []

    # ----------------- Public methods -----------------

    def is_integration_client(self) -> bool:
        return False

    def log(self, dataset_name: str, df: pd.DataFrame, _batch_size: int = 1000):
        if df.shape[0] == 0:
            print(f"Skipping log of empty dataframe for dataset {dataset_name}")
            return

        if df.shape[0] > _batch_size:
            print(
                "Warning: Dataframe is too large, consider using a small dataframe"
            )

        if dataset_name not in self.dataset_requests:
            return FakeResponse(404, f"Dataset {dataset_name} not found")
        dataset_req = self.dataset_requests[dataset_name]
        timestamp_field = self.datasets[dataset_name].timestamp_field
        if timestamp_field not in df.columns:
            return FakeResponse(
                400,
                f"Timestamp field {timestamp_field} not found in dataframe "
                f"while logging to dataset `{dataset_name}`",
            )
        if str(df[timestamp_field].dtype) != "datetime64[ns]":
            return FakeResponse(
                400,
                f"Timestamp field {timestamp_field} is not of type "
                f"datetime64[ns] but found {df[timestamp_field].dtype} in "
                f"dataset {dataset_name}",
            )
        # Check if the dataframe has the same schema as the dataset
        schema = dataset_req.dsschema
        exceptions = data_schema_check(schema, df)
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
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
    ):
        self._reset()
        if datasets is None:
            datasets = []
        if featuresets is None:
            featuresets = []
        for dataset in datasets:
            if not isinstance(dataset, Dataset):
                raise TypeError(
                    f"Expected a list of datasets, got `{dataset.__name__}`"
                    f" of type `{type(dataset)}` instead."
                )
            self.dataset_requests[dataset._name] = dataset_to_proto(dataset)
            self.datasets[dataset._name] = _DatasetInfo(
                [f.name for f in dataset.fields],
                dataset.key_fields,
                dataset.timestamp_field,
                dataset.on_demand,
            )
            for pipeline in dataset._pipelines:
                for input in pipeline.inputs:
                    self.listeners[input._name].append(pipeline)
        for featureset in featuresets:
            if not isinstance(featureset, Featureset):
                raise TypeError(
                    f"Expected a list of featuresets, got `{featureset.__name__}`"
                    f" of type `{type(featureset)}` instead."
                )
            self.featureset_requests[featureset._name] = featureset_to_proto(
                featureset
            )
            self.features_for_fs[featureset._name] = features_from_fs(
                featureset
            )
            # Check if the dataset used by the extractor is registered
            for extractor in featureset.extractors:
                datasets = [
                    x._name for x in extractor.get_dataset_dependencies()
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
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = "default",
        sampling_rate: Optional[float] = 1.0,
    ) -> pd.DataFrame:
        if log:
            raise NotImplementedError("log is not supported in MockClient")
        if input_dataframe.empty:
            return pd.DataFrame()
        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn_)
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn_ for f in input_feature.features]
                )
        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )
        extractors = get_extractor_order(
            input_feature_list, output_feature_list, self.extractors
        )
        timestamps = pd.Series([datetime.utcnow()] * len(input_dataframe))
        return self._run_extractors(
            extractors, input_dataframe, output_feature_list, timestamps
        )

    def extract_historical_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_dataframe: pd.DataFrame,
        timestamps: pd.Series,
    ) -> Union[pd.DataFrame, pd.Series]:
        if input_dataframe.empty:
            return pd.DataFrame()
        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn_)
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn_ for f in input_feature.features]
                )
        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )
        extractors = get_extractor_order(
            input_feature_list, output_feature_list, self.extractors
        )
        return self._run_extractors(
            extractors, input_dataframe, output_feature_list, timestamps
        )

    # ----------------- Private methods -----------------

    def _prepare_extractor_args(
        self, extractor: Extractor, intermediate_data: Dict[str, pd.Series]
    ):
        args = []
        for input in extractor.inputs:
            if isinstance(input, Feature):
                if input.fqn_ in intermediate_data:
                    args.append(intermediate_data[input.fqn_])
                else:
                    raise Exception(
                        f"Feature `{input}` could not be "
                        f"calculated by any extractor."
                    )
            elif isinstance(input, Featureset):
                raise Exception(
                    "Featureset is not supported as input to an "
                    "extractor since they are mutable."
                )
            elif type(input) == tuple:
                series = []
                for feature in input:
                    if feature.fqn_ in intermediate_data:
                        series.append(intermediate_data[feature.fqn_])
                    else:
                        raise Exception(
                            f"Feature {feature.fqn_} couldn't be "
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
        timestamps: pd.Series,
    ):
        # Map of feature name to the pandas series
        intermediate_data: Dict[str, pd.Series] = {}
        for col in input_df.columns:
            intermediate_data[col] = input_df[col]
        for extractor in extractors:
            prepare_args = self._prepare_extractor_args(
                extractor, intermediate_data
            )
            features = self.features_for_fs[extractor.featureset]
            feature_schema = {}
            for feature in features:
                feature_schema[
                    f"{extractor.featureset}.{feature.name}"
                ] = feature.dtype
            fields = []
            for feature_str in extractor.output_features:
                feature_str = f"{extractor.featureset}.{feature_str}"
                if feature_str not in feature_schema:
                    raise ValueError(f"Feature {feature_str} not found")
                dtype = feature_schema[feature_str]
                fields.append(Field(name=feature_str, dtype=dtype))
            dsschema = DSSchema(
                values=Schema(fields=fields)
            )  # stuff every field as value

            allowed_datasets = [
                x._name for x in extractor.get_dataset_dependencies()
            ]
            fennel.datasets.datasets.dataset_lookup = partial(
                dataset_lookup_impl,
                self.data,
                self.datasets,
                allowed_datasets,
                extractor.name,
            )
            output = extractor.bound_func(timestamps, *prepare_args)
            fennel.datasets.datasets.dataset_lookup = partial(
                dataset_lookup_impl, self.data, self.datasets, None, None
            )
            if not isinstance(output, (pd.Series, pd.DataFrame)):
                raise Exception(
                    f"Extractor `{extractor.name}` returned "
                    f"invalid type `{type(output)}`, expected a pandas series or dataframe"
                )
            output_df = pd.DataFrame(output)
            exceptions = data_schema_check(dsschema, output_df)
            if len(exceptions) > 0:
                raise Exception(
                    f"Extractor `{extractor.name}` returned "
                    f"invalid schema: {exceptions}"
                )
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
        for output_feature in output_feature_list:
            if isinstance(output_feature, Feature):
                output_df[output_feature.fqn_] = intermediate_data[
                    output_feature.fqn_
                ]
            elif isinstance(output_feature, Featureset):
                for f in output_feature.features:
                    output_df[f.fqn_] = intermediate_data[f.fqn_]
            elif type(output_feature) == tuple:
                for f in output_feature:
                    output_df[f.fqn_] = intermediate_data[f.fqn_]
            else:
                raise Exception(
                    f"Unknown feature type {type(output_feature)} found "
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
        self.dataset_requests: Dict[str, CoreDataset] = {}
        self.featureset_requests: Dict[str, CoreFeatureset] = {}
        self.features_for_fs: Dict[str, List[ProtoFeature]] = {}
        self.datasets: Dict[str, _DatasetInfo] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl, self.data, self.datasets, None, None
        )
        self.extractors: List[Extractor] = []


def mock_client(test_func):
    def wrapper(*args, **kwargs):
        f = True
        if "data_integration" not in test_func.__name__:
            client = MockClient()
            f = test_func(*args, **kwargs, client=client)
        if (
            "USE_INT_CLIENT" in os.environ
            and int(os.environ.get("USE_INT_CLIENT")) == 1
        ):
            print("Running rust client tests")
            client = IntegrationClient()
            f = test_func(*args, **kwargs, client=client)
        return f

    return wrapper
