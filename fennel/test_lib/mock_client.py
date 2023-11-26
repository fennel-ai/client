from __future__ import annotations

import copy
import json
import os
import sys
import types
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from functools import partial
import logging

import numpy as np
import pandas as pd
from frozendict import frozendict
from typing import Callable, Dict, List, Tuple, Optional, Union

import fennel.datasets.datasets
import fennel.sources as sources
from fennel._vendor.requests import Response  # type: ignore
from fennel.client import Client
from fennel.datasets import Dataset, field, Pipeline, OnDemand  # noqa
from fennel.datasets.datasets import sync_validation_for_pipelines
from fennel.featuresets import Featureset, Feature, Extractor, is_valid_feature
from fennel.featuresets.featureset import sync_validation_for_extractors
from fennel.gen.dataset_pb2 import CoreDataset
from fennel.gen.featureset_pb2 import CoreFeatureset
from fennel.gen.featureset_pb2 import (
    Feature as ProtoFeature,
    Extractor as ProtoExtractor,
    ExtractorType as ProtoExtractorType,
)
from fennel.gen.schema_pb2 import Field, DSSchema, Schema
from fennel.lib.graph_algorithms import (
    get_extractor_order,
    is_extractor_graph_cyclic,
)
from fennel.lib.includes import includes  # noqa
from fennel.lib.schema import data_schema_check, get_datatype
from fennel.lib.to_proto import (
    dataset_to_proto,
    features_from_fs,
    extractors_from_fs,
    featureset_to_proto,
)
from fennel.test_lib.executor import Executor
from fennel.test_lib.integration_client import IntegrationClient
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


def dataset_lookup_impl(
    data: Dict[str, pd.DataFrame],
    aggregated_datasets: Dict,
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
            f"Dataset `{cls_name}` not found, please ensure it is synced."
        )
    if allowed_datasets is not None and cls_name not in allowed_datasets:
        raise ValueError(
            f"Extractor `{extractor_name}` is not allowed to access dataset "
            f"`{cls_name}`, enabled datasets are {allowed_datasets}. "
            f"Use `depends_on` param in @extractor to specify dataset "
            f"dependencies."
        )
    join_columns = keys.columns.tolist()
    if keys.isnull().values.any():
        null_rows = keys[keys.isnull().any(axis=1)]
        raise ValueError(
            f"Null values found in key fields {join_columns}\n. Eg {null_rows}"
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
    if cls_name not in data and cls_name not in aggregated_datasets:
        logger.warning(
            f"Not data found for Dataset `{cls_name}` during lookup, returning an empty dataframe"
        )
        # Create a dataframe with all nulls
        val_cols = datasets[cls_name].fields
        if len(fields) > 0:
            val_cols = [x for x in val_cols if x in fields]
        empty_df = pd.DataFrame(
            columns=val_cols, data=[[None] * len(val_cols)] * len(keys)
        )
        return empty_df, pd.Series(np.array([False] * len(keys)))

    timestamp_field = datasets[cls_name].timestamp_field
    timestamp_length = len(ts)
    if timestamp_length != keys.shape[0]:
        raise ValueError(
            f"Length of timestamp array `{timestamp_length}` does not match ",
            f"length of keys array {keys.shape[0]} for dataset {cls_name}.",
        )
    keys = keys.reset_index(drop=True)
    ts = ts.reset_index(drop=True)
    assert keys.shape[0] == len(
        ts
    ), "Length of keys and ts should be same " "found {} and {}".format(
        keys.shape[0], len(ts)
    )
    keys[timestamp_field] = ts

    keys[FENNEL_ORDER] = np.arange(len(keys))
    # Sort the keys by timestamp
    keys = keys.sort_values(timestamp_field)
    if cls_name in aggregated_datasets:
        data_dict = aggregated_datasets[cls_name]
        # Gather all the columns that are needed from data_dict to create a df.
        result_dfs = []
        for col, right_df in data_dict.items():
            right_df[FENNEL_LOOKUP] = True
            right_df[FENNEL_TIMESTAMP] = right_df[timestamp_field]
            # Cast the column in keys to the same dtype as the column in right_df
            for col in keys:
                if col in right_df and keys[col].dtype != right_df[col].dtype:
                    keys[col] = keys[col].astype(right_df[col].dtype)
            try:
                df = pd.merge_asof(
                    left=keys,
                    right=right_df,
                    on=timestamp_field,
                    by=join_columns,
                    direction="backward",
                    suffixes=("", "_right"),
                )
            except Exception as e:
                raise ValueError(
                    f"Error while performing lookup on dataset {cls_name} "
                    f"with key fields {join_columns}, key length "
                    f"{keys.shape}, and shape of dataset being "
                    f"looked up {right_df.shape}: {e} "
                )
            df.drop(timestamp_field, axis=1, inplace=True)
            df = df.set_index(FENNEL_ORDER).loc[np.arange(len(df)), :]
            result_dfs.append(df)
        # Get common columns
        common_columns = set(result_dfs[0].columns)
        for df in result_dfs[1:]:
            common_columns.intersection_update(df.columns)

        # Remove common columns from all DataFrames except the first one
        for i in range(1, len(result_dfs)):
            result_dfs[i] = result_dfs[i].drop(columns=common_columns)

        # Concatenate the DataFrames column-wise
        df = pd.concat(result_dfs, axis=1)
    else:
        right_df = data[cls_name]
        right_df[FENNEL_LOOKUP] = True
        right_df[FENNEL_TIMESTAMP] = right_df[timestamp_field]
        for col in keys:
            if col in right_df and keys[col].dtype != right_df[col].dtype:
                keys[col] = keys[col].astype(right_df[col].dtype)
        try:
            df = pd.merge_asof(
                left=keys,
                right=right_df,
                on=timestamp_field,
                by=join_columns,
                direction="backward",
                suffixes=("", "_right"),
            )
        except Exception as e:
            raise ValueError(
                f"Error while performing lookup on dataset {cls_name} "
                f"with key fields {join_columns}, key length "
                f"{keys.shape}, and shape of dataset being"
                f"looked up{right_df.shape}: {e} "
            )
        df.drop(timestamp_field, axis=1, inplace=True)
        df.rename(columns={FENNEL_TIMESTAMP: timestamp_field}, inplace=True)
        df = df.set_index(FENNEL_ORDER).loc[np.arange(len(df)), :]
    found = df[FENNEL_LOOKUP].apply(lambda x: x is not np.nan)
    df.drop(columns=[FENNEL_LOOKUP], inplace=True)
    right_df.drop(columns=[FENNEL_LOOKUP], inplace=True)
    if len(fields) > 0:
        df = df[fields]
    df = df.reset_index(drop=True)
    return df, found


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


@dataclass
class _DatasetInfo:
    fields: List[str]
    key_fields: List[str]
    timestamp_field: str
    is_source_dataset: bool
    on_demand: OnDemand

    def empty_df(self):
        return pd.DataFrame(columns=self.fields)


class MockClient(Client):
    def __init__(self):
        self.dataset_requests: Dict[str, CoreDataset] = {}
        self.featureset_requests: Dict[str, CoreFeatureset] = {}
        self.features_for_fs: Dict[str, List[ProtoFeature]]
        self.extractor_funcs: Dict[str, Callable] = {}
        self.dataset_info: Dict[str, _DatasetInfo] = {}
        self.datasets: Dict[str, Dataset] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        self.aggregated_datasets: Dict = {}
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl,
            self.data,
            self.aggregated_datasets,
            self.dataset_info,
            None,
            None,
        )
        self.webhook_to_dataset_map: Dict[str, List[str]] = defaultdict(list)
        self.extractors: List[Extractor] = []

    # ----------------- Debug methods -----------------------------------------

    def get_dataset_df(self, dataset_name: str) -> pd.DataFrame:
        if dataset_name not in self.dataset_info:
            raise ValueError(f"Dataset `{dataset_name}` not found")

        # If we haven't seen any values for this dataset, return an empty df with the right schema.
        if (
            dataset_name not in self.data
            and dataset_name not in self.aggregated_datasets
        ):
            return self.dataset_info[dataset_name].empty_df()

        if dataset_name in self.data:
            return copy.deepcopy(self.data[dataset_name])

        # This must be an aggregated dataset
        key_fields = self.dataset_info[dataset_name].key_fields
        ts_field = self.dataset_info[dataset_name].timestamp_field
        required_fields = key_fields + [ts_field]
        column_wise_df = self.aggregated_datasets[dataset_name]
        key_dfs = pd.DataFrame()
        # Collect all timestamps across all columns
        for data in column_wise_df.values():
            subset_df = data[required_fields]
            key_dfs = pd.concat([key_dfs, subset_df], ignore_index=True)
            key_dfs.drop_duplicates(inplace=True)

        # Find the values for all columns as of the timestamp in key_dfs
        extrapolated_dfs = []
        for col, data in column_wise_df.items():
            df = pd.merge_asof(
                left=key_dfs,
                right=data,
                on=ts_field,
                by=key_fields,
                direction="backward",
                suffixes=("", "_right"),
            )
            extrapolated_dfs.append(df)
        # Merge all the extrapolated dfs, column wise and drop duplicate columns
        df = pd.concat(extrapolated_dfs, axis=1)
        df = df.loc[:, ~df.columns.duplicated()]
        if FENNEL_LOOKUP in df.columns:
            df.drop(columns=[FENNEL_LOOKUP], inplace=True)
        if FENNEL_TIMESTAMP in df.columns:
            df.drop(columns=[FENNEL_TIMESTAMP], inplace=True)
        return df

    # ----------------- Public methods -----------------------------------------

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        _batch_size: int = 1000,
    ):
        if df.shape[0] == 0:
            print(f"Skipping log of empty dataframe for webhook {webhook}")
            return FakeResponse(200, "OK")

        webhook_endpoint = f"{webhook}:{endpoint}"
        if webhook_endpoint not in self.webhook_to_dataset_map:
            return FakeResponse(
                404, f"Webhook endpoint {webhook_endpoint} not " f"found"
            )
        for ds in self.webhook_to_dataset_map[webhook_endpoint]:
            resp = self._internal_log(ds, df)
            if resp.status_code != 200:
                return resp
        return FakeResponse(200, "OK")

    def sync(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        tier: Optional[str] = None,
    ):
        self._reset()
        if datasets is None:
            datasets = []
        if featuresets is None:
            featuresets = []

        input_datasets_for_pipelines = defaultdict(list)
        for dataset in datasets:
            if not isinstance(dataset, Dataset):
                raise TypeError(
                    f"Expected a list of datasets, got `{dataset.__name__}`"
                    f" of type `{type(dataset)}` instead."
                )
            self.dataset_requests[dataset._name] = dataset_to_proto(dataset)
            if hasattr(dataset, sources.SOURCE_FIELD):
                self._process_data_connector(dataset, tier)

            self.datasets[dataset._name] = dataset
            is_source_dataset = hasattr(dataset, sources.SOURCE_FIELD)
            self.dataset_info[dataset._name] = _DatasetInfo(
                [f.name for f in dataset.fields],
                dataset.key_fields,
                dataset.timestamp_field,
                is_source_dataset,
                dataset.on_demand,
            )
            if (
                not self.dataset_requests[dataset._name].is_source_dataset
                and len(dataset._pipelines) == 0
            ):
                raise ValueError(
                    f"Dataset {dataset._name} has no pipelines and is not a source dataset"
                )
            selected_pipelines = [
                x
                for x in dataset._pipelines
                if x.tier.is_entity_selected(tier) and x.active
            ]
            sync_validation_for_pipelines(selected_pipelines, dataset._name)

            for pipeline in selected_pipelines:
                for input in pipeline.inputs:
                    input_datasets_for_pipelines[input._name].append(
                        f"{pipeline._dataset_name}.{pipeline.name}"
                    )
                    self.listeners[input._name].append(pipeline)

        # Check that input_datasets_for_pipelines is a subset of self.datasets.
        for ds, pipelines in input_datasets_for_pipelines.items():
            if ds not in self.datasets:
                raise ValueError(
                    f"Dataset `{ds}` is an input to the pipelines: `{pipelines}` but is not synced. Please add it to the sync call."
                )
        for featureset in featuresets:
            if not isinstance(featureset, Featureset):
                raise TypeError(
                    f"Expected a list of featuresets, got `{featureset.__name__}`"
                    f" of type `{type(featureset)}` instead."
                )
            self.features_for_fs[featureset._name] = features_from_fs(
                featureset
            )
            self.featureset_requests[featureset._name] = featureset_to_proto(
                featureset
            )
            # Check if the dataset used by the extractor is registered
            for extractor in featureset.extractors:
                if not extractor.tiers.is_entity_selected(tier):
                    continue
                datasets = [
                    x._name for x in extractor.get_dataset_dependencies()
                ]
                for dataset in datasets:
                    if dataset not in self.dataset_requests:
                        raise ValueError(
                            f"Dataset `{dataset}` not found in sync call"
                        )
            self.extractors.extend(
                [
                    x
                    for x in featureset.extractors
                    if x.tiers.is_entity_selected(tier)
                ]
            )
        fs_obj_map = {
            featureset._name: featureset for featureset in featuresets
        }

        for featureset in featuresets:
            proto_extractors = extractors_from_fs(featureset, fs_obj_map, tier)
            for extractor in proto_extractors:
                if extractor.extractor_type != ProtoExtractorType.PY_FUNC:
                    continue
                extractor_fqn = f"{featureset._name}.{extractor.name}"
                self.extractor_funcs[extractor_fqn] = get_extractor_func(
                    extractor
                )

        if is_extractor_graph_cyclic(self.extractors):
            raise Exception("Cyclic graph detected in extractors")
        return FakeResponse(200, "OK")

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset, str]],
        output_feature_list: List[Union[Feature, Featureset, str]],
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
            elif isinstance(input_feature, str) and is_valid_feature(
                input_feature
            ):
                input_feature_names.append(input_feature)
            elif isinstance(input_feature, Featureset):
                raise Exception(
                    "Providing a featureset as input is deprecated. "
                    f"List the features instead. {[f.fqn() for f in input_feature.features]}."
                )
        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )
        for input_col, feature in zip(
            input_dataframe.columns, input_feature_list
        ):
            if isinstance(feature, str):
                continue
            col_type = get_datatype(feature.dtype)  # type: ignore
            input_dataframe[input_col] = cast_col_to_dtype(
                input_dataframe[input_col], col_type
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
        input_feature_list: List[Union[Feature, Featureset, str]],
        output_feature_list: List[Union[Feature, Featureset, str]],
        timestamp_column: str,
        format: str = "pandas",
        input_dataframe: Optional[pd.DataFrame] = None,
        output_bucket: Optional[str] = None,
        output_prefix: Optional[str] = None,
        input_bucket: Optional[str] = None,
        input_prefix: Optional[str] = None,
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
        assert input_bucket is None, "input_bucket is not supported"
        assert input_prefix is None, "input_prefix is not supported"
        assert output_bucket is None, "output_bucket is not supported"
        assert output_prefix is None, "output_prefix is not supported"

        if input_dataframe.empty:
            return pd.DataFrame()
        timestamps = input_dataframe[timestamp_column]
        timestamps = pd.to_datetime(timestamps)
        input_feature_names = []
        for inp_feature in input_feature_list:
            if isinstance(inp_feature, Feature):
                input_feature_names.append(inp_feature.fqn_)
            elif isinstance(inp_feature, str) and is_valid_feature(inp_feature):
                input_feature_names.append(inp_feature)
            elif isinstance(inp_feature, Featureset):
                raise Exception(
                    "Providing a featureset as input is deprecated. "
                    f"List the features instead. {[f.fqn() for f in inp_feature.features]}."
                )
        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )
        for input_col, feature in zip(
            input_dataframe.columns, input_feature_list
        ):
            if isinstance(feature, str):
                continue
            col_type = get_datatype(feature.dtype)  # type: ignore
            input_dataframe[input_col] = cast_col_to_dtype(
                input_dataframe[input_col], col_type
            )
        extractors = get_extractor_order(
            input_feature_list, output_feature_list, self.extractors
        )
        output_df = self._run_extractors(
            extractors, input_dataframe, output_feature_list, timestamps
        )
        assert output_df.shape[0] == len(timestamps), (
            f"Output dataframe has {output_df.shape[0]} rows, but there are only {len(timestamps)} "
            "timestamps"
        )
        output_df[timestamp_column] = timestamps
        return output_df

    def extract_historical_features_progress(self, request_id):
        return FakeResponse(404, "Extract historical features not supported")

    def extract_historical_cancel_request(self, request_id):
        return FakeResponse(404, "Extract historical features not supported")

    # --------------- Public MockClient Specific methods -------------------

    def sleep(self, seconds: float = 0):
        pass

    def integration_mode(self):
        return "mock"

    def is_integration_client(self) -> bool:
        return False

    # ----------------- Private methods --------------------------------------

    def _process_data_connector(self, dataset: Dataset, tier):
        connector = getattr(dataset, sources.SOURCE_FIELD)
        connector = connector if isinstance(connector, list) else [connector]
        connector = [x for x in connector if x.tiers.is_entity_selected(tier)]
        if len(connector) > 1:
            raise ValueError(
                f"Dataset `{dataset._name}` has more than one source defined, found {len(connector)} sources."
            )
        if len(connector) == 0:
            return
        connector = connector[0]
        if isinstance(connector, sources.WebhookConnector):
            src = connector.data_source
            webhook_endpoint = f"{src.name}:{connector.endpoint}"
            self.webhook_to_dataset_map[webhook_endpoint].append(dataset._name)

    def _internal_log(self, dataset_name: str, df: pd.DataFrame):
        if df.shape[0] == 0:
            print(
                f"Skipping log of empty dataframe for webhook `{dataset_name}`"
            )
            return FakeResponse(200, "OK")

        if dataset_name not in self.dataset_requests:
            return FakeResponse(404, f"Dataset `{dataset_name}` not found")

        dataset_req = self.dataset_requests[dataset_name]
        timestamp_field = self.dataset_info[dataset_name].timestamp_field
        if timestamp_field not in df.columns:
            return FakeResponse(
                400,
                f"Timestamp field `{timestamp_field}` not found in dataframe "
                f"while logging to dataset `{dataset_name}`",
            )
        for col in df.columns:
            # If any of the columns is a dictionary, convert it to a frozen dict
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(lambda x: frozendict(x))
        # Check if the dataframe has the same schema as the dataset
        schema = dataset_req.dsschema
        try:
            df = cast_df_to_schema(df, schema)
        except Exception as e:
            return FakeResponse(
                400,
                f"Schema validation failed during data insertion to `{dataset_name}`: {str(e)}",
            )
        if str(df[timestamp_field].dtype) != "datetime64[ns]":
            return FakeResponse(
                400,
                f"Timestamp field {timestamp_field} is not of type "
                f"datetime64[ns] but found {df[timestamp_field].dtype} in "
                f"dataset {dataset_name}",
            )
        exceptions = data_schema_check(schema, df, dataset_name)
        if len(exceptions) > 0:
            return FakeResponse(
                400,
                f"Schema validation failed during data insertion to `{dataset_name}`"
                f" {str(exceptions)}",
            )
        self._merge_df(df, dataset_name)
        for pipeline in self.listeners[dataset_name]:
            executor = Executor(self.data)
            try:
                ret = executor.execute(
                    pipeline, self.datasets[pipeline._dataset_name]
                )
            except Exception as e:
                return FakeResponse(
                    400,
                    f"Error while executing pipeline {pipeline.name} "
                    f"in dataset {dataset_name}: {str(e)}",
                )
            if ret is None:
                continue
            if ret.is_aggregate:
                # Aggregate pipelines are not logged
                self.aggregated_datasets[pipeline.dataset_name] = ret.agg_result
                continue

            # Recursively log the output of the pipeline to the datasets
            resp = self._internal_log(pipeline.dataset_name, ret.df)
            if resp.status_code != 200:
                return resp
        return FakeResponse(200, "OK")

    def _prepare_extractor_args(
        self, extractor: Extractor, intermediate_data: Dict[str, pd.Series]
    ):
        args = []
        for input in extractor.inputs:
            if isinstance(input, Feature):
                if input.fqn_ in intermediate_data:
                    if (
                        intermediate_data[input.fqn_]
                        .apply(lambda x: isinstance(x, dict))
                        .any()
                    ):
                        intermediate_data[input.fqn_] = intermediate_data[
                            input.fqn_
                        ].apply(lambda x: frozendict(x))
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
            elif type(input) is tuple:
                series = []
                for feature in input:
                    if feature.fqn_ in intermediate_data:
                        series.append(intermediate_data[feature.fqn_])
                    else:
                        raise Exception(
                            f"Feature {feature.fqn_} couldn't be "
                            f"calculated by any extractor."
                        )
                if series.apply(lambda x: isinstance(x, dict)).any():
                    series = series.apply(lambda x: frozendict(x))
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
        output_feature_list: List[Union[Feature, Featureset, str]],
        timestamps: pd.Series,
    ):
        # Map of feature name to the pandas series
        intermediate_data: Dict[str, pd.Series] = {}
        for col in input_df.columns:
            if input_df[col].apply(lambda x: isinstance(x, dict)).any():
                input_df[col] = input_df[col].apply(lambda x: frozendict(x))
            intermediate_data[col] = input_df[col].reset_index(drop=True)
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
                    raise ValueError(f"Feature `{feature_str}` not found")
                dtype = feature_schema[feature_str]
                fields.append(Field(name=feature_str, dtype=dtype))
            dsschema = DSSchema(
                values=Schema(fields=fields)
            )  # stuff every field as value

            if extractor.extractor_type == ProtoExtractorType.ALIAS:
                feature_name = extractor.fqn_output_features()[0]
                intermediate_data[feature_name] = intermediate_data[
                    extractor.inputs[0].fqn()
                ]
                intermediate_data[feature_name].name = feature_name
                self._check_schema_exceptions(
                    intermediate_data[feature_name], dsschema, extractor.name
                )
                continue

            if extractor.extractor_type == ProtoExtractorType.LOOKUP:
                output = self._compute_lookup_extractor(
                    extractor, timestamps.copy(), intermediate_data
                )
                self._check_schema_exceptions(output, dsschema, extractor.name)
                continue

            allowed_datasets = [
                x._name for x in extractor.get_dataset_dependencies()
            ]
            fennel.datasets.datasets.dataset_lookup = partial(
                dataset_lookup_impl,
                self.data,
                self.aggregated_datasets,
                self.dataset_info,
                allowed_datasets,
                extractor.name,
            )
            extractor_fqn = f"{extractor.featureset}.{extractor.name}"
            func = self.extractor_funcs[extractor_fqn]
            try:
                ts_clone = timestamps.copy()
                output = func(ts_clone, *prepare_args)
            except Exception as e:
                raise Exception(
                    f"Extractor `{extractor.name}` in `{extractor.featureset}` "
                    f"failed to run with error: {e}. "
                )
            fennel.datasets.datasets.dataset_lookup = partial(
                dataset_lookup_impl,
                self.data,
                self.aggregated_datasets,
                self.dataset_info,
                None,
                None,
            )
            if not isinstance(output, (pd.Series, pd.DataFrame)):
                raise Exception(
                    f"Extractor `{extractor.name}` returned "
                    f"invalid type `{type(output)}`, expected a pandas series or dataframe"
                )
            self._check_schema_exceptions(output, dsschema, extractor.name)
            if isinstance(output, pd.Series):
                if output.name in intermediate_data:
                    continue
                # If output is a dict, convert it to frozendict
                if output.apply(lambda x: isinstance(x, dict)).any():
                    output = frozendict(output)
                intermediate_data[output.name] = output
            elif isinstance(output, pd.DataFrame):
                for col in output.columns:
                    if col in intermediate_data:
                        continue
                    if output[col].apply(lambda x: isinstance(x, dict)).any():
                        output[col] = output[col].apply(frozendict)
                    intermediate_data[col] = output[col]
            else:
                raise Exception(
                    f"Extractor {extractor.name} returned "
                    f"invalid type {type(output)}"
                )
        # Ensure the  number of rows in each column is the same
        num_rows_per_col = {
            col: len(intermediate_data[col]) for col in intermediate_data
        }
        first_col = list(num_rows_per_col.keys())[0]
        for col, num_rows in num_rows_per_col.items():
            if num_rows != num_rows_per_col[first_col]:
                raise Exception(
                    f"Number of values in feature {col} is {num_rows}, "
                    f"but {num_rows_per_col[first_col]} in feature {first_col}. "
                )

        # Prepare the output dataframe
        output_df = pd.DataFrame()
        for out_feature in output_feature_list:
            if isinstance(out_feature, Feature):
                output_df[out_feature.fqn_] = intermediate_data[
                    out_feature.fqn_
                ]
            elif isinstance(out_feature, str) and is_valid_feature(out_feature):
                output_df[out_feature] = intermediate_data[out_feature]
            elif isinstance(out_feature, Featureset):
                for f in out_feature.features:
                    output_df[f.fqn_] = intermediate_data[f.fqn_]
            elif type(out_feature) is tuple:
                for f in out_feature:
                    output_df[f.fqn_] = intermediate_data[f.fqn_]
            else:
                raise Exception(
                    f"Unknown feature {out_feature} of type {type(out_feature)} found "
                    f"during feature extraction."
                )
        return output_df

    def _check_schema_exceptions(
        self, output, dsschema: DSSchema, extractor_name: str
    ):
        if output is None or output.shape[0] == 0:
            return
        output_df = pd.DataFrame(output)
        output_df.reset_index(inplace=True)
        exceptions = data_schema_check(dsschema, output_df, extractor_name)
        if len(exceptions) > 0:
            raise Exception(
                f"Extractor `{extractor_name}` returned "
                f"invalid schema for data: {exceptions}"
            )

    def _compute_lookup_extractor(
        self,
        extractor: Extractor,
        timestamps: pd.Series,
        intermediate_data: Dict[str, pd.Series],
    ) -> pd.Series:
        if len(extractor.output_features) != 1:
            raise ValueError(
                f"Lookup extractor {extractor.name} must have exactly one output feature, found {len(extractor.output_features)}"
            )
        if len(extractor.depends_on) != 1:
            raise ValueError(
                f"Lookup extractor {extractor.name} must have exactly one dependent dataset, found {len(extractor.depends_on)}"
            )

        input_features = {
            k.name: intermediate_data[k] for k in extractor.inputs  # type: ignore
        }
        allowed_datasets = [
            x._name for x in extractor.get_dataset_dependencies()
        ]
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl,
            self.data,
            self.aggregated_datasets,
            self.dataset_info,
            allowed_datasets,
            extractor.name,
        )
        results, _ = extractor.depends_on[0].lookup(
            timestamps, **input_features
        )
        if (
            not extractor.derived_extractor_info
            or not extractor.derived_extractor_info.field
            or not extractor.derived_extractor_info.field.name
        ):
            raise TypeError(
                f"Field for lookup extractor {extractor.name} must have a named field"
            )
        results = results[extractor.derived_extractor_info.field.name]
        if extractor.derived_extractor_info.default is not None:
            if results.dtype != object:
                results = results.fillna(
                    extractor.derived_extractor_info.default
                )
            else:
                # fillna doesn't work for list type or dict type :cols
                for row in results.loc[results.isnull()].index:
                    results[row] = extractor.derived_extractor_info.default
            results = cast_col_to_dtype(
                results,
                get_datatype(extractor.derived_extractor_info.field.dtype),
            )
        else:
            results = cast_col_to_dtype(
                results,
                get_datatype(
                    Optional[extractor.derived_extractor_info.field.dtype]
                ),
            )
            results.replace({np.nan: None}, inplace=True)

        results.name = extractor.fqn_output_features()[0]
        intermediate_data[extractor.fqn_output_features()[0]] = results
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl,
            self.data,
            self.aggregated_datasets,
            self.dataset_info,
            None,
            None,
        )

        return results

    def _merge_df(self, df: pd.DataFrame, dataset_name: str):
        if not self.dataset_info[dataset_name].is_source_dataset:
            # If it's a derived dataset, just replace the data, since we
            # recompute the entire pipeline on every run.
            timestamp_field = self.dataset_info[dataset_name].timestamp_field
            self.data[dataset_name] = df.sort_values(timestamp_field)
            return

        # Filter the dataframe to only include the columns in the schema
        columns = self.dataset_info[dataset_name].fields
        input_columns = df.columns.tolist()
        # Check that input columns are a subset of the dataset columns
        if not set(columns).issubset(set(input_columns)):
            raise ValueError(
                f"Dataset columns {columns} are not a subset of "
                f"Input columns {input_columns}"
            )
        df = df[columns]

        if len(self.dataset_info[dataset_name].key_fields) > 0:
            df = df.sort_values(self.dataset_info[dataset_name].timestamp_field)
            try:
                df = df.groupby(
                    self.dataset_info[dataset_name].key_fields, as_index=False
                ).last()
            except Exception:
                # This happens when struct fields are present in the key fields
                # Convert key fields to string, group by and then drop the key
                # column
                df["__fennel__key__"] = df[
                    self.dataset_info[dataset_name].key_fields
                ].apply(lambda x: str(dict(x)), axis=1)
                df = df.groupby("__fennel__key__", as_index=False).last()
                df = df.drop(columns="__fennel__key__")
            df = df.reset_index(drop=True)

        if dataset_name in self.data:
            df = pd.concat([self.data[dataset_name], df])

        # Sort by timestamp
        timestamp_field = self.dataset_info[dataset_name].timestamp_field
        self.data[dataset_name] = df.sort_values(timestamp_field)

    def _reset(self):
        self.dataset_requests: Dict[str, CoreDataset] = {}
        self.features_for_fs: Dict[str, List[ProtoFeature]] = {}
        self.extractor_funcs: Dict[str, ProtoExtractor] = {}
        self.dataset_info: Dict[str, _DatasetInfo] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        fennel.datasets.datasets.dataset_lookup = partial(
            dataset_lookup_impl,
            self.data,
            self.aggregated_datasets,
            self.dataset_info,
            None,
            None,
        )
        self.extractors: List[Extractor] = []


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


def cast_df_to_schema(df: pd.DataFrame, dsschema: DSSchema) -> pd.DataFrame:
    # Handle fields in keys and values
    fields = list(dsschema.keys.fields) + list(dsschema.values.fields)
    df = df.copy()
    df = df.reset_index(drop=True)
    for f in fields:
        if f.name not in df.columns:
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
            client = IntegrationClient(mode)
            f = test_func(*args, **kwargs, client=client)
        return f

    return wrapper
