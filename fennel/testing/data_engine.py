import copy
import logging
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Callable, Any

import numpy as np
import pandas as pd
from frozendict import frozendict

import fennel.datasets.datasets
import fennel.gen.schema_pb2 as schema_proto
from fennel.datasets import Dataset, Pipeline
from fennel.datasets.datasets import sync_validation_for_pipelines
from fennel.gen.dataset_pb2 import CoreDataset
from fennel.internal_lib.duration import Duration, duration_to_timedelta
from fennel.internal_lib.schema import data_schema_check
from fennel.internal_lib.to_proto import dataset_to_proto
from fennel.sources import sources, PreProcValue
from fennel.testing.executor import Executor
from fennel.testing.test_utils import (
    FakeResponse,
    cast_df_to_schema,
    cast_col_to_dtype,
)

TEST_PORT = 50051
TEST_DATA_PORT = 50052
FENNEL_LOOKUP = "__fennel_lookup_exists__"
FENNEL_ORDER = "__fennel_order__"
FENNEL_TIMESTAMP = "__fennel_timestamp__"

logger = logging.getLogger(__name__)


def _preproc_df(
    df: pd.DataFrame, pre_proc: Dict[str, sources.PreProcValue]
) -> pd.DataFrame:
    new_df = df.copy()
    for col, pre_proc_value in pre_proc.items():
        if isinstance(pre_proc_value, sources.Ref):
            col_name = pre_proc_value.name
            if col_name not in df.columns:
                raise ValueError(
                    f"Referenced column {col_name} not found in dataframe"
                )
            new_df[col] = df[col_name]
        else:
            new_df[col] = pre_proc_value
    return new_df


@dataclass
class _Dataset:
    fields: List[str]
    is_source_dataset: bool
    core_dataset: CoreDataset
    dataset: Dataset
    bounded: bool
    data: Optional[pd.DataFrame] = None
    pre_proc: Optional[Dict[str, sources.PreProcValue]] = None
    aggregated_datasets: Optional[Dict[str, Any]] = None
    idleness: Optional[Duration] = None
    prev_log_time: Optional[datetime] = None

    def empty_df(self):
        return pd.DataFrame(columns=self.fields)


class DataEngine(object):
    def __init__(self):
        self.datasets: Dict[str, _Dataset] = {}
        self.webhook_to_dataset_map: Dict[str, List[str]] = defaultdict(list)
        self.dataset_listeners: Dict[str, List[Pipeline]] = defaultdict(list)

        fennel.datasets.datasets.dataset_lookup = partial(
            self._dataset_lookup_impl,
            None,
            None,
        )

    def get_datasets(self) -> List[Dataset]:
        return [value.dataset for value in self.datasets.values()]

    def get_dataset_fields(self, dataset_name: str) -> List[str]:
        """
        Returns list of dataset fields apart from keyed fields and timestamp field.
        Args:
            dataset_name: (str) - Name of the dataset.
        Returns:
            List[str] - List of names of the fields
        """
        return self.datasets[dataset_name].fields

    def get_dataset(self, dataset_name: str) -> Dataset:
        """
        Get Dataset object from dataset name.
        Args:
            dataset_name: (str) - Name of the dataset.
        Returns:
            Dataset - Dataset object
        """
        return self.datasets[dataset_name].dataset

    def get_dataset_df(self, dataset_name: str) -> pd.DataFrame:
        """
        Return pandas dataframe from dataset name
        Args:
            dataset_name: (str) - Name of the dataset
        Returns:
            Pandas dataframe
        """
        if dataset_name not in self.datasets:
            raise ValueError(f"Dataset `{dataset_name}` not found")

        # If we haven't seen any values for this dataset, return an empty df with the right schema.
        if (
            not isinstance(self.datasets[dataset_name].data, pd.DataFrame)
            and not self.datasets[dataset_name].aggregated_datasets
        ):
            return self.datasets[dataset_name].empty_df()

        if isinstance(self.datasets[dataset_name].data, pd.DataFrame):
            df = copy.deepcopy(self.datasets[dataset_name].data)
            if FENNEL_LOOKUP in df.columns:  # type: ignore
                df.drop(columns=[FENNEL_LOOKUP], inplace=True)  # type: ignore
            if FENNEL_TIMESTAMP in df.columns:  # type: ignore
                df.drop(columns=[FENNEL_TIMESTAMP], inplace=True)  # type: ignore
            return df

        # This must be an aggregated dataset
        key_fields = self.datasets[dataset_name].dataset.key_fields
        ts_field = self.datasets[dataset_name].dataset.timestamp_field
        required_fields = key_fields + [ts_field]
        column_wise_df = self.datasets[dataset_name].aggregated_datasets
        key_dfs = pd.DataFrame()
        # Collect all timestamps across all columns
        for data in column_wise_df.values():  # type: ignore
            subset_df = data[required_fields]
            key_dfs = pd.concat([key_dfs, subset_df], ignore_index=True)
            key_dfs.drop_duplicates(inplace=True)
        # Sort key_dfs by timestamp
        key_dfs.sort_values(ts_field, inplace=True)
        # Find the values for all columns as of the timestamp in key_dfs
        extrapolated_dfs = []
        for col, data in column_wise_df.items():  # type: ignore
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

    def add_datasets(
        self,
        datasets: List[Dataset],
        tier: Optional[str] = None,
    ):
        """
        This method is used during sync to add datasets to the data engine.
        Args:
            datasets: List[Datasets] - List of datasets to add to the data engine
            tier: Optional[str] - Tier against which datasets will be added.
        Returns:
            None
        """
        input_datasets_for_pipelines = defaultdict(list)
        for dataset in datasets:
            if not isinstance(dataset, Dataset):
                raise TypeError(
                    f"Expected a list of datasets, got `{dataset.__name__}`"
                    f" of type `{type(dataset)}` instead."
                )
            core_dataset = dataset_to_proto(dataset)
            if hasattr(dataset, sources.SOURCE_FIELD):
                pre_proc, bounded, idleness = self._process_data_connector(
                    dataset, tier
                )
            else:
                pre_proc, bounded, idleness = None, False, None

            is_source_dataset = hasattr(dataset, sources.SOURCE_FIELD)
            fields = [f.name for f in dataset.fields]

            self.datasets[dataset._name] = _Dataset(
                fields=fields,
                is_source_dataset=is_source_dataset,
                core_dataset=core_dataset,
                dataset=dataset,
                bounded=bounded,
                pre_proc=pre_proc,
                idleness=idleness,
                prev_log_time=datetime.utcnow(),
            )

            if (
                not core_dataset.is_source_dataset
                and len(dataset._pipelines) == 0
            ):
                raise ValueError(
                    f"Dataset {dataset._name} has no pipelines and is not a source dataset"
                )
            selected_pipelines = [
                x for x in dataset._pipelines if x.tier.is_entity_selected(tier)
            ]
            sync_validation_for_pipelines(selected_pipelines, dataset._name)

            for pipeline in selected_pipelines:
                for input in pipeline.inputs:
                    input_datasets_for_pipelines[input._name].append(
                        f"{pipeline._dataset_name}.{pipeline.name}"
                    )
                    self.dataset_listeners[input._name].append(pipeline)

        # Check that input_datasets_for_pipelines is a subset of self.datasets.
        for ds, pipelines in input_datasets_for_pipelines.items():
            if ds not in self.datasets:
                raise ValueError(
                    f"Dataset `{ds}` is an input to the pipelines: `{pipelines}` but is not synced. Please add it to the sync call."
                )

    def get_dataset_names(self) -> List[str]:
        """
        Returns list of dataset names stored in the data engine.
        Returns:
            List[str]
        """
        return list(self.datasets.keys())

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        _batch_size: int = 1000,
    ) -> FakeResponse:
        if df.shape[0] == 0:
            print(f"Skipping log of empty dataframe for webhook {webhook}")
            return FakeResponse(200, "OK")

        webhook_endpoint = f"{webhook}:{endpoint}"
        if webhook_endpoint not in self.webhook_to_dataset_map:
            return FakeResponse(
                404, f"Webhook endpoint {webhook_endpoint} not " f"found"
            )
        for ds in self.webhook_to_dataset_map[webhook_endpoint]:
            try:
                schema = self.datasets[ds].core_dataset.dsschema
                if self.datasets[ds].pre_proc is not None:
                    pre_proc_cols = list(
                        self.datasets[ds].pre_proc.keys()  # type: ignore
                    )
                else:
                    pre_proc_cols = []
                df = cast_df_to_schema(df, schema, pre_proc_cols)
            except Exception as e:
                raise Exception(
                    f"Schema validation failed during data insertion to `{ds}`: {str(e)}",
                )
            resp = self._internal_log(ds, df)
            if resp.status_code != 200:
                return resp
        return FakeResponse(200, "OK")

    def lookup(self, dataset_name: str, ts: pd.Series, *args, **kwargs):
        if dataset_name not in self.datasets:
            raise KeyError(f"Dataset: {dataset_name} not found")

        fennel.datasets.datasets.dataset_lookup = partial(
            self._dataset_lookup_impl,
            [dataset_name],
            None,
        )

        timestamps = cast_col_to_dtype(
            ts,
            schema_proto.DataType(timestamp_type=schema_proto.TimestampType()),
        )

        dataframe, found = self.datasets[dataset_name].dataset.lookup(
            timestamps,
            *args,
            **kwargs,
        )

        fennel.datasets.datasets.dataset_lookup = partial(
            self._dataset_lookup_impl,
            None,
            None,
        )
        return dataframe, found

    def get_dataset_lookup_impl(
        self,
        extractor_name: Optional[str],
        allowed_datasets: Optional[List[str]],
    ) -> Callable:
        """
        Return the lookup implementation function that be monkey-patched during lookup
        Args:
            extractor_name: (Optional[str]) - Name of the extractor calling the lookup function.
            allowed_datasets: (Optional[List[str]]) - List of allowed datasets that an extractor can lookup from.
        Returns:
            Callable - The lookup implementation
        """
        return partial(
            self._dataset_lookup_impl,
            extractor_name,
            allowed_datasets,
        )

    def _dataset_lookup_impl(
        self,
        extractor_name: Optional[str],
        allowed_datasets: Optional[List[str]],
        cls_name: str,
        ts: pd.Series,
        fields: List[str],
        keys: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        if cls_name not in self.datasets:
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
        right_key_fields = self.datasets[cls_name].dataset.key_fields
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
        if (
            not isinstance(self.datasets[cls_name].data, pd.DataFrame)
            and not self.datasets[cls_name].aggregated_datasets
        ):
            logger.warning(
                f"Not data found for Dataset `{cls_name}` during lookup, returning an empty dataframe"
            )
            # Create a dataframe with all nulls
            val_cols = self.datasets[cls_name].fields
            if len(fields) > 0:
                val_cols = [x for x in val_cols if x in fields]
            empty_df = pd.DataFrame(
                columns=val_cols, data=[[None] * len(val_cols)] * len(keys)
            )
            return empty_df, pd.Series(np.array([False] * len(keys)))

        timestamp_field = self.datasets[cls_name].dataset.timestamp_field
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
        if self.datasets[cls_name].aggregated_datasets:
            data_dict = self.datasets[cls_name].aggregated_datasets
            # Gather all the columns that are needed from data_dict to create a df.
            result_dfs = []
            for col, right_df in data_dict.items():  # type: ignore
                try:
                    df = self._as_of_lookup(
                        cls_name, keys, right_df, join_columns, timestamp_field
                    )
                except ValueError as err:
                    raise ValueError(err)
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
            right_df = self.datasets[cls_name].data
            try:
                df = self._as_of_lookup(
                    cls_name, keys, right_df, join_columns, timestamp_field
                )
            except ValueError as err:
                raise ValueError(err)
            df.rename(columns={FENNEL_TIMESTAMP: timestamp_field}, inplace=True)
            df = df.set_index(FENNEL_ORDER).loc[np.arange(len(df)), :]

        found = df[FENNEL_LOOKUP].apply(lambda x: x is not np.nan)
        df.drop(columns=[FENNEL_LOOKUP], inplace=True)
        if len(fields) > 0:
            df = df[fields]
        df = df.reset_index(drop=True)
        return df, found

    def _as_of_lookup(
        self,
        dataset_name: str,
        keys: pd.DataFrame,
        right_df: pd.DataFrame,
        join_columns: List[str],
        timestamp_field: str,
    ) -> pd.DataFrame:
        """
        This function does as-of lookup on the right dataframe using keys dataframe.
        The as-of lookup is done on join_columns and using timestamps in timestamp_field.
        Args:
            dataset_name: (str) - Name of the Dataset on which we have to do the lookup.
            keys: (pd.DataFrame) - Dataframe containing the keys on which lookup will be done.
            right_df: (pd.DataFrame) - Dataframe of the Dataset.
            join_columns: (List[str]) - Columns on which we have to do the join.
            timestamp_field: str - Name of the timestamp column present in keys.

        Returns:
            pd.DataFrame - Dataset
        """
        right_df[FENNEL_LOOKUP] = True
        right_df[FENNEL_TIMESTAMP] = right_df[timestamp_field]
        cols_to_replace = []
        for col in keys:
            # Cast the column in keys to the same dtype as the column in right_df
            if col in right_df and keys[col].dtype != right_df[col].dtype:
                keys[col] = keys[col].astype(right_df[col].dtype)

            # Changing dtype of Struct to str for making it hashable
            if col in right_df and right_df[col].dtype == object:
                cols_to_replace.append(col)
                right_df[f"{col}__internal"] = right_df[col].apply(
                    lambda x: str(dict(x))
                )
                keys[f"{col}__internal"] = keys[col].apply(
                    lambda x: str(dict(x))
                )

        right_df = right_df.drop(columns=cols_to_replace)
        new_join_columns = []
        for col in join_columns:
            if col in cols_to_replace:
                new_join_columns.append(f"{col}__internal")
            else:
                new_join_columns.append(col)
        try:
            df = pd.merge_asof(
                left=keys,
                right=right_df,
                on=timestamp_field,
                by=new_join_columns,
                direction="backward",
                suffixes=("", "_right"),
            )
            df.drop(
                [f"{col}__internal" for col in cols_to_replace],
                axis=1,
                inplace=True,
            )
        except Exception as e:
            raise ValueError(
                f"Error while performing lookup on dataset {dataset_name} "
                f"with key fields {join_columns}, key length "
                f"{keys.shape}, and shape of dataset being "
                f"looked up {right_df.shape}: {e} "
            )
        df.drop(timestamp_field, axis=1, inplace=True)
        return df

    def _internal_log(self, dataset_name: str, df: pd.DataFrame):
        if df.shape[0] == 0:
            print(
                f"Skipping log of empty dataframe for webhook `{dataset_name}`"
            )
            return FakeResponse(200, "OK")

        if dataset_name not in self.datasets:
            raise ValueError(f"Dataset `{dataset_name}` not found")

        bounded = self.datasets[dataset_name].bounded
        prev_log_time = self.datasets[dataset_name].prev_log_time
        if bounded and prev_log_time:
            idleness = self.datasets[dataset_name].idleness
            if not idleness:
                raise ValueError(
                    "Idleness parameter should be non-empty for bounded source"
                )
            expected_idleness_secs = duration_to_timedelta(
                idleness
            ).total_seconds()
            actual_idleness_secs = (
                datetime.utcnow() - prev_log_time
            ).total_seconds()
            # Do not log the data if a bounded source is idle for more time than expected
            if actual_idleness_secs >= expected_idleness_secs:
                print(
                    f"Skipping log of dataframe for webhook `{dataset_name}` since the source is closed"
                )
                return FakeResponse(200, "OK")

        for col in df.columns:
            # If any of the columns is a dictionary, convert it to a frozen dict
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(lambda x: frozendict(x))

        # If pre_proc for the dataset is set, transform the dataframe using it
        pre_proc = self.datasets[dataset_name].pre_proc
        if pre_proc is not None:
            try:
                df = _preproc_df(df, pre_proc)
            except ValueError as e:
                raise ValueError(
                    f"Error using pre_proc for dataset `{dataset_name}`: {str(e)}",
                )

        core_dataset = self.datasets[dataset_name].core_dataset
        timestamp_field = self.datasets[dataset_name].dataset.timestamp_field
        if timestamp_field not in df.columns:
            raise ValueError(
                f"Timestamp field `{timestamp_field}` not found in dataframe "
                f"while logging to dataset `{dataset_name}`",
            )

        # Check if the dataframe has the same schema as the dataset
        schema = core_dataset.dsschema
        if str(df[timestamp_field].dtype) != "datetime64[ns]":
            raise ValueError(
                400,
                f"Timestamp field {timestamp_field} is not of type "
                f"datetime64[ns] but found {df[timestamp_field].dtype} in "
                f"dataset {dataset_name}",
            )
        exceptions = data_schema_check(schema, df, dataset_name)
        if len(exceptions) > 0:
            raise ValueError(
                f"Schema validation failed during data insertion to `{dataset_name}`"
                f" {str(exceptions)}",
            )
        self._merge_df(df, dataset_name)
        for pipeline in self.dataset_listeners[dataset_name]:
            executor = Executor(
                {
                    name: self.datasets[name].data
                    for name in self.datasets
                    if isinstance(self.datasets[name].data, pd.DataFrame)
                }
            )
            try:
                ret = executor.execute(
                    pipeline, self.datasets[pipeline._dataset_name].dataset
                )
            except Exception as e:
                raise Exception(
                    f"Error while executing pipeline `{pipeline.name}` "
                    f"in dataset `{dataset_name}`: {str(e)}",
                )
            if ret is None:
                continue
            if ret.is_aggregate:
                # Aggregate pipelines are not logged
                self.datasets[pipeline.dataset_name].aggregated_datasets = (
                    ret.agg_result
                )
                continue

            # Recursively log the output of the pipeline to the datasets
            resp = self._internal_log(pipeline.dataset_name, ret.df)
            if resp.status_code != 200:
                return resp
        return FakeResponse(200, "OK")

    def _merge_df(self, df: pd.DataFrame, dataset_name: str):
        if not self.datasets[dataset_name].is_source_dataset:
            # If it's a derived dataset, just replace the data, since we
            # recompute the entire pipeline on every run.
            timestamp_field = self.datasets[
                dataset_name
            ].dataset.timestamp_field
            self.datasets[dataset_name].data = df.sort_values(timestamp_field)
            return

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

        if len(self.datasets[dataset_name].dataset.key_fields) > 0:
            df = df.sort_values(
                self.datasets[dataset_name].dataset.timestamp_field
            )
            try:
                df = df.groupby(
                    self.datasets[dataset_name].dataset.key_fields,
                    as_index=False,
                ).last()
            except Exception:
                # This happens when struct fields are present in the key fields
                # Convert key fields to string, group by and then drop the key
                # column
                df["__fennel__key__"] = df[
                    self.datasets[dataset_name].dataset.key_fields
                ].apply(lambda x: str(dict(x)), axis=1)
                df = df.groupby("__fennel__key__", as_index=False).last()
                df = df.drop(columns="__fennel__key__")
            df = df.reset_index(drop=True)

        if isinstance(self.datasets[dataset_name].data, pd.DataFrame):
            df = pd.concat([self.datasets[dataset_name].data, df])

        # Sort by timestamp
        timestamp_field = self.datasets[dataset_name].dataset.timestamp_field
        self.datasets[dataset_name].data = df.sort_values(timestamp_field)
        self.datasets[dataset_name].prev_log_time = datetime.utcnow()

    def _process_data_connector(
        self, dataset: Dataset, tier: Optional[str] = None
    ) -> Tuple[Optional[Dict[str, PreProcValue]], bool, Optional[Duration]]:
        connector = getattr(dataset, sources.SOURCE_FIELD)
        connector = connector if isinstance(connector, list) else [connector]
        connector = [x for x in connector if x.tiers.is_entity_selected(tier)]
        if len(connector) > 1:
            raise ValueError(
                f"Dataset `{dataset._name}` has more than one source defined, found {len(connector)} sources."
            )
        if len(connector) == 0:
            return None, False, None
        connector = connector[0]
        if isinstance(connector, sources.WebhookConnector):
            src = connector.data_source
            webhook_endpoint = f"{src.name}:{connector.endpoint}"
            self.webhook_to_dataset_map[webhook_endpoint].append(dataset._name)
            return connector.pre_proc, connector.bounded, connector.idleness
        return None, False, None
