import copy
import logging
import math
import types
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
from typing import Dict, List, Optional, Tuple, Callable, Any

import numpy as np
import pandas as pd
import pyarrow as pa
from frozendict import frozendict

import fennel.datasets.datasets
from fennel.connectors import (
    connectors,
    Webhook,
    DataSource,
)
from fennel.datasets import (
    Aggregate,
    Average,
    Dataset,
    Max,
    Min,
    Pipeline,
    Quantile,
    Stddev,
)
from fennel.datasets.datasets import (
    get_index,
    IndexDuration,
)
from fennel.expr.expr import Expr, TypedExpr
from fennel.gen import schema_pb2 as schema_proto
from fennel.gen.dataset_pb2 import CoreDataset
from fennel.internal_lib.duration import Duration, duration_to_timedelta
from fennel.internal_lib.schema import data_schema_check, get_datatype
from fennel.internal_lib.to_proto import (
    dataset_to_proto,
    get_dataset_core_code,
    to_includes_proto,
    wrap_function,
)
from fennel.internal_lib.utils import parse_datetime
from fennel.lib.includes import EnvSelector
from fennel.testing.execute_aggregation import FENNEL_DELETE_TIMESTAMP
from fennel.testing.executor import Executor
from fennel.testing.test_utils import (
    FakeResponse,
    cast_df_to_schema,
    cast_df_to_pandas_dtype,
    add_deletes,
    FENNEL_LOOKUP,
    FENNEL_TIMESTAMP,
    FENNEL_ORDER,
    cast_col_to_arrow_dtype,
    cast_col_to_pandas_dtype,
)

TEST_PORT = 50051
TEST_DATA_PORT = 50052

logger = logging.getLogger(__name__)
internal_webhook = Webhook(name="_internal_fennel_webhook")
internal_env = "testing"


def gen_dataset_webhook_endpoint(ds_name: str) -> str:
    return f"_internal_{ds_name}_endpoint"


def ds_from_endpoint(endpoint: str) -> str:
    return endpoint.split("_internal_")[1].split("_endpoint")[0]


def internal_webhook_endpoint(dataset: Dataset):
    src = internal_webhook.endpoint(gen_dataset_webhook_endpoint(dataset._name))
    src.envs = EnvSelector(internal_env)
    if len(dataset.key_fields) > 0:
        src.cdc = "upsert"
    else:
        src.cdc = "append"
    src.disorder = "14d"
    return src


def _preproc_df(
    dataset: Dataset,
    df: pd.DataFrame,
    pre_proc: Dict[str, connectors.PreProcValue],
) -> pd.DataFrame:
    new_df = df.copy()
    schema = dataset.schema()
    # Remove the fields that are created in preproc
    for col in pre_proc.keys():
        if col in schema:
            del schema[col]

    for col, pre_proc_value in pre_proc.items():
        if isinstance(pre_proc_value, connectors.Ref):
            col_name = pre_proc_value.name
            if col_name not in df.columns:
                raise ValueError(
                    f"Referenced column {col_name} not found in dataframe"
                )
            new_df[col] = df[col_name]
        elif isinstance(pre_proc_value, connectors.Eval):
            input_schema = copy.deepcopy(schema)

            # try casting the column in dataframe according to schema defined in additional schema in eval preproc
            if pre_proc_value.additional_schema:
                for name, dtype in pre_proc_value.additional_schema.items():
                    if name not in df.columns:
                        raise ValueError(
                            f"Field `{name}` defined in schema for eval preproc not found in dataframe."
                        )
                    try:
                        new_df[name] = cast_col_to_arrow_dtype(
                            new_df[name], get_datatype(dtype)
                        )
                        new_df[name] = cast_col_to_pandas_dtype(
                            new_df[name], get_datatype(dtype)
                        )
                    except Exception as e:
                        raise ValueError(
                            f"Casting column in dataframe for field `{name}` defined in schema for eval preproc failed : {e}"
                        )
                    input_schema[name] = dtype

            try:
                if isinstance(pre_proc_value.eval_type, Expr):
                    new_df[col] = pre_proc_value.eval_type.eval(
                        new_df, input_schema
                    )
                elif isinstance(pre_proc_value.eval_type, TypedExpr):
                    new_df[col] = pre_proc_value.eval_type.expr.eval(
                        new_df, input_schema
                    )
                else:
                    assign_func_pycode = wrap_function(
                        dataset._name,
                        get_dataset_core_code(dataset),
                        "",
                        to_includes_proto(pre_proc_value.eval_type),
                        column_name=col,
                        is_assign=True,
                    )
                    assign_df = new_df.copy()
                    mod = types.ModuleType(assign_func_pycode.entry_point)
                    code = (
                        assign_func_pycode.imports
                        + "\n"
                        + assign_func_pycode.generated_code
                    )
                    exec(code, mod.__dict__)
                    func = mod.__dict__[assign_func_pycode.entry_point]
                    new_df = func(assign_df)
            except Exception as e:
                raise Exception(
                    f"Error in assign preproc for field `{col}` for dataset `{dataset._name}`: {e} "
                )
        else:
            if isinstance(pre_proc_value, datetime):
                new_df[col] = parse_datetime(pre_proc_value)
            else:
                new_df[col] = pre_proc_value
    return new_df


def _preproc_where_df(
    df: pd.DataFrame, filter_fn: Callable, ds: Dataset
) -> pd.DataFrame:
    filter_func_pycode = wrap_function(
        ds._name,
        get_dataset_core_code(ds),
        "",
        to_includes_proto(filter_fn),
        is_filter=True,
    )
    new_df = df.copy()
    mod = types.ModuleType(filter_func_pycode.entry_point)
    code = filter_func_pycode.imports + "\n" + filter_func_pycode.generated_code
    exec(code, mod.__dict__)
    func = mod.__dict__[filter_func_pycode.entry_point]
    try:
        new_df = func(new_df)
    except Exception as e:
        raise Exception(
            f"Error in filter function `{filter_fn.__name__}` for preproc , {e}"
        )
    return new_df


@dataclass
class _SrcInfo:
    webhook_endpoint: str
    preproc: Optional[Dict[str, connectors.PreProcValue]]
    bounded: bool
    idleness: Optional[Duration]
    prev_log_time: Optional[datetime] = None
    where: Optional[Callable] = None


@dataclass
class _Dataset:
    fields: List[str]
    is_source_dataset: bool
    core_dataset: CoreDataset
    dataset: Dataset
    srcs: List[_SrcInfo]
    data: Optional[pd.DataFrame] = None
    erased_keys: List[Dict[str, Any]] = field(default_factory=list)
    # We use this sore default values of fields passed in aggregates
    default_values: Dict[str, Any] = field(default_factory=dict)

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
            False,
        )

    def get_datasets(self) -> List[Dataset]:
        return [value.dataset for value in self.datasets.values()]

    def get_dataset_fields(self, dataset_name: str) -> List[str]:
        """
        Returns list of dataset fields.
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
        if not isinstance(self.datasets[dataset_name].data, pd.DataFrame):
            return self.datasets[dataset_name].empty_df()

        else:
            df = copy.deepcopy(self.datasets[dataset_name].data)
            if FENNEL_LOOKUP in df.columns:  # type: ignore
                df.drop(columns=[FENNEL_LOOKUP], inplace=True)  # type: ignore
            if FENNEL_TIMESTAMP in df.columns:  # type: ignore
                df.drop(columns=[FENNEL_TIMESTAMP], inplace=True)  # type: ignore
            if FENNEL_DELETE_TIMESTAMP in df.columns:  # type: ignore
                df.drop(columns=[FENNEL_DELETE_TIMESTAMP], inplace=True)  # type: ignore
            return df

    def add_datasets(
        self,
        datasets: List[Dataset],
        incremental: bool = False,
        env: Optional[str] = None,
    ):
        """
        This method is used during sync to add datasets to the data engine.
        Args:
            incremental: bool - whether to incrementally add the dataset or not.
            datasets: List[Datasets] - List of datasets to add to the data engine.
            env: Optional[str] - Tier against which datasets will be added.
        Returns:
            None
        """
        input_datasets_for_pipelines = defaultdict(list)
        for dataset in datasets:
            core_dataset = dataset_to_proto(dataset)
            if len(dataset._pipelines) == 0:
                srcs = self._process_data_connector(dataset, incremental, env)
            else:
                srcs = []

            is_source_dataset = hasattr(dataset, connectors.SOURCE_FIELD)
            fields = [f.name for f in dataset.fields]

            if dataset._name not in self.datasets:
                self.datasets[dataset._name] = _Dataset(
                    fields=fields,
                    is_source_dataset=is_source_dataset,
                    core_dataset=core_dataset,
                    dataset=dataset,
                    srcs=srcs,
                    erased_keys=[],
                )
            else:
                older_core_dataset = self.datasets[dataset._name].core_dataset
                if core_dataset.version > older_core_dataset.version:
                    # Reset the things related to the dataset because the version increased,
                    # could be only possible via incremental.
                    self.datasets[dataset._name] = _Dataset(
                        fields=fields,
                        is_source_dataset=is_source_dataset,
                        core_dataset=core_dataset,
                        dataset=dataset,
                        srcs=srcs,
                        erased_keys=[],
                    )

            selected_pipelines = [
                x for x in dataset._pipelines if x.env.is_entity_selected(env)
            ]

            for pipeline in selected_pipelines:
                for input in pipeline.inputs:
                    input_datasets_for_pipelines[input._name].append(
                        f"{pipeline._dataset_name}.{pipeline.name}"
                    )
                    if pipeline not in self.dataset_listeners[input._name]:
                        self.dataset_listeners[input._name].append(pipeline)

            # Add default values of fields if passed in terminal_node
            self._add_default_values(dataset._name, selected_pipelines)

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
                # Run preproc-transform
                preproc = [
                    x.preproc
                    for x in self.datasets[ds].srcs
                    if x.webhook_endpoint == webhook_endpoint
                ]

                if len(preproc) > 0 and preproc[0] is not None:
                    assert (
                        len(preproc) == 1
                    ), f"Multiple preproc found for {ds} and {webhook_endpoint}"
                    try:
                        df = _preproc_df(
                            self.datasets[ds].dataset, df, preproc[0]
                        )
                    except ValueError as e:
                        raise ValueError(
                            f"Error using pre_proc for dataset `{ds}`: {str(e)}",
                        )

                # Run preproc-filter
                wheres = [
                    x.where
                    for x in self.datasets[ds].srcs
                    if x.webhook_endpoint == webhook_endpoint
                ]
                if len(wheres) > 0 and wheres[0] is not None:
                    assert (
                        len(wheres) == 1
                    ), f"Multiple preproc wheres found for {ds} and {webhook_endpoint}"
                    try:
                        df = _preproc_where_df(
                            df, wheres[0], self.datasets[ds].dataset
                        )
                    except ValueError as e:
                        raise ValueError(
                            f"Error using filter pre_proc for dataset `{ds}`: {str(e)}",
                        )
                # Cast output to pyarrow dtypes
                df = cast_df_to_schema(df, schema)

            except Exception as e:
                raise Exception(
                    f"Schema validation failed during data insertion to `{ds}`: {str(e)}",
                )
            resp = self._internal_log(ds, df)
            if resp.status_code != 200:
                return resp
        return FakeResponse(200, "OK")

    def log_to_dataset(
        self,
        dataset: Dataset,
        df: pd.DataFrame,
    ) -> FakeResponse:
        if df.shape[0] == 0:
            print(
                f"Skipping log of empty dataframe for dataset {dataset._name}"
            )
            return FakeResponse(200, "OK")

        if dataset._name not in self.datasets:
            raise ValueError(f"Dataset `{dataset._name}` not found")

        schema = self.datasets[dataset._name].core_dataset.dsschema
        df = cast_df_to_schema(df, schema)
        resp = self._internal_log(dataset._name, df)
        return resp

    def erase(self, dataset_name: str, erase_keys: List[Dict[str, Any]]):
        if dataset_name not in self.datasets:
            return FakeResponse(404, f"Dataset {dataset_name} not " f"found")
        schema = self.datasets[dataset_name].core_dataset.dsschema
        for erase_key in erase_keys:
            for field_name in erase_key.keys():
                all_fields = list(schema.keys.fields) + list(
                    schema.values.fields
                )
                if field_name not in [f.name for f in all_fields]:
                    raise ValueError(f"Field: {field_name} not in dataset")
                if field_name not in [f.name for f in schema.keys.fields]:
                    raise ValueError(
                        f"Field: {field_name} not in a key field in dataset"
                    )
                if field_name not in schema.erase_keys:
                    raise ValueError(
                        f"Field: {field_name} is not an erase key field in dataset"
                    )
            if len(erase_key.keys()) != len(schema.erase_keys):
                raise ValueError("Not enough erase key were provided")

        self.datasets[dataset_name].erased_keys.extend(erase_keys)

        self._filter_erase_key(dataset_name)

        return FakeResponse(200, "OK")

    def get_dataset_lookup_impl(
        self,
        extractor_name: Optional[str],
        allowed_datasets: Optional[List[str]],
        use_as_of: bool,
    ) -> Callable:
        """
        Return the lookup implementation function that be monkey-patched during lookup
        Args:
            extractor_name: (Optional[str]) - Name of the extractor calling the lookup function.
            allowed_datasets: (Optional[List[str]]) - List of allowed datasets that an extractor can lookup from.
            use_as_of: (bool) - Whether to do offline or online lookup
        Returns:
            Callable - The lookup implementation
        """
        return partial(
            self._dataset_lookup_impl,
            extractor_name,
            allowed_datasets,
            use_as_of,
        )

    def _add_default_values(
        self, dataset_name: str, selected_pipelines: List[Pipeline]
    ):
        if dataset_name not in self.datasets:
            raise ValueError(f"Dataset `{dataset_name}` not found.")

        if len(selected_pipelines) == 0 or not isinstance(
            selected_pipelines[0].terminal_node, Aggregate
        ):
            return

        terminal_node: Aggregate = selected_pipelines[0].terminal_node
        for aggregate in terminal_node.aggregates:
            if isinstance(aggregate, (Average, Min, Max, Quantile, Stddev)):
                self.datasets[dataset_name].default_values[
                    aggregate.into_field
                ] = aggregate.default

    def _filter_erase_key(
        self,
        dataset_name: str,
    ):
        def is_kept(row):
            for erase_key in self.datasets[dataset_name].erased_keys:
                match_all = True
                for erase_col in erase_key.keys():
                    if row[erase_col] != erase_key[erase_col]:
                        match_all = False
                        break
                if match_all:
                    return False
            return True

        data = self.datasets[dataset_name].data

        if data is not None:
            self.datasets[dataset_name].data = data[data.apply(is_kept, axis=1)]

    def _dataset_lookup_impl(
        self,
        extractor_name: Optional[str],
        allowed_datasets: Optional[List[str]],
        use_as_of: bool,
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

        index = get_index(self.datasets[cls_name].dataset)

        if index is None:
            raise ValueError(
                f"Please define an index on dataset : {cls_name} for loookup"
            )

        if use_as_of and index.offline == IndexDuration.none:
            raise ValueError(
                f"Please define an offline index on dataset : {cls_name}"
            )

        if not use_as_of and not index.online:
            raise ValueError(
                f"Please define an online index on dataset : {cls_name}"
            )

        if use_as_of and index.offline == IndexDuration.none:
            raise ValueError(
                f"Please define an offline index on dataset : {cls_name}"
            )

        join_columns = keys.columns.tolist()
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
        if not isinstance(self.datasets[cls_name].data, pd.DataFrame):
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
            empty_df = empty_df
            return self._cast_after_lookup(cls_name, empty_df), pd.Series(
                np.array([False] * len(keys))
            )

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
        right_df = self.datasets[cls_name].data
        try:
            df = self._as_of_lookup(
                cls_name, keys, right_df, join_columns, timestamp_field
            )
            df = self._materialize_intermediate_df(df, ts)
        except ValueError as err:
            raise ValueError(err)
        df.rename(columns={FENNEL_TIMESTAMP: timestamp_field}, inplace=True)
        df = df.set_index(FENNEL_ORDER).loc[np.arange(len(df)), :]
        found = df[FENNEL_LOOKUP].apply(lambda x: x is not np.nan)
        df.drop(columns=[FENNEL_LOOKUP], inplace=True)
        if len(fields) > 0:
            df = df[fields]
        df = df.reset_index(drop=True)
        # Drop all hidden columns
        df = df.loc[:, ~df.columns.str.startswith("__fennel")]
        return self._cast_after_lookup(cls_name, df), found

    def _materialize_intermediate_df(self, df: pd.DataFrame, ts: pd.Series):
        """
        For some cases such as Exponential Decay Aggregation the column in the dataframe only stores
        an intermediate state of the computation. This function materializes the intermediate state
        to the final state that is to be returned.
        """
        for col in df.columns:
            if not col.endswith("@@internal_state"):
                continue

            _, col_name, intermediate_col_type, _ = col.split("@@")
            res = []
            if intermediate_col_type == "exp_decay":
                for i in range(len(df)):
                    val = df[col].iloc[i]
                    # Intermediate state is a list of 4 values
                    if not isinstance(val, list) or len(val) != 4:
                        res.append(pd.NA)
                        continue
                    log_sum_pos, log_sum_neg, max_exponent, decay = val
                    reduced_val = 0.0
                    timestamp_sec = ts.iloc[i].timestamp()
                    exp = max_exponent + log_sum_pos - (timestamp_sec * decay)
                    reduced_val += math.exp(exp) if exp > -25.0 else 0.0
                    exp = max_exponent + log_sum_neg - (timestamp_sec * decay)
                    reduced_val -= math.exp(exp) if exp > -25.0 else 0.0
                    res.append(reduced_val)
                df[col_name] = res
            else:
                raise ValueError(
                    f"Intermediate column type {intermediate_col_type} not supported"
                )
        return df

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
            if col in right_df and (
                right_df[col].dtype == object
                or "map" in str(right_df[col].dtype)
                or "struct" in str(right_df[col].dtype)
            ):
                cols_to_replace.append(col)
                # Python v3.9 and 3.10 have different behavior for str(dict(x))
                # as compared to 3.11 and 3.12.
                # So we first try to convert the column to str directly, and
                # if that fails, we conditionally check if the column is a dict.
                try:
                    right_df[f"{col}__internal"] = right_df[col].apply(
                        lambda x: str(dict(x))
                    )
                    keys[f"{col}__internal"] = keys[col].apply(
                        lambda x: str(dict(x))
                    )
                except Exception:
                    right_df[f"{col}__internal"] = right_df[col].apply(
                        lambda x: str(dict(x)) if isinstance(x, dict) else x
                    )
                    keys[f"{col}__internal"] = keys[col].apply(
                        lambda x: str(dict(x)) if isinstance(x, dict) else x
                    )

        right_df = right_df.drop(columns=cols_to_replace)
        new_join_columns = []
        for col in join_columns:
            if col in cols_to_replace:
                new_join_columns.append(f"{col}__internal")
            else:
                new_join_columns.append(col)

        # Sort the right_df by timestamp
        right_df = right_df.sort_values(timestamp_field)
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

        for src in self.datasets[dataset_name].srcs:
            bounded = src.bounded
            prev_log_time = src.prev_log_time
            if bounded and prev_log_time:
                idleness = src.idleness
                if not idleness:
                    raise ValueError(
                        "Idleness parameter should be non-empty for bounded source"
                    )
                expected_idleness_secs = duration_to_timedelta(
                    idleness
                ).total_seconds()
                actual_idleness_secs = (
                    datetime.now(timezone.utc) - prev_log_time
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

        core_dataset = self.datasets[dataset_name].core_dataset
        timestamp_field = self.datasets[dataset_name].dataset.timestamp_field
        if timestamp_field not in df.columns:
            raise ValueError(
                f"Timestamp field `{timestamp_field}` not found in dataframe "
                f"while logging to dataset `{dataset_name}`",
            )

        # Check if the dataframe has the same schema as the dataset
        schema = core_dataset.dsschema
        if df[timestamp_field].dtype != pd.ArrowDtype(
            pa.timestamp("ns", "UTC")
        ):
            raise ValueError(
                400,
                f"Timestamp field {timestamp_field} is not of type "
                f"datetime64[ns, UTC] but found {df[timestamp_field].dtype} in "
                f"dataset {dataset_name}",
            )
        exceptions = data_schema_check(schema, df, dataset_name)
        if len(exceptions) > 0:
            raise ValueError(
                f"Schema validation failed during data insertion to `{dataset_name}`"
                f" {str(exceptions)}",
            )

        dataset = self.get_dataset(dataset_name)
        if len(dataset.key_fields) > 0:
            # It is an upsert dataset, so insert appropriate deletes
            df = self._add_deletes(df, dataset)

        self._merge_df(df, dataset_name)
        for pipeline in self.dataset_listeners[dataset_name]:
            executor = Executor(
                {
                    name: copy.deepcopy(self.datasets[name].data)
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
                    f"in dataset `{self.datasets[pipeline._dataset_name].dataset._name}`: {str(e)}",
                )
            if ret is None:
                continue

            # Recursively log the output of the pipeline to the datasets
            resp = self._internal_log(pipeline.dataset_name, ret.df)
            if resp.status_code != 200:
                return resp

        self._filter_erase_key(dataset_name)
        return FakeResponse(200, "OK")

    def _add_deletes(self, df: pd.DataFrame, dataset: Dataset) -> pd.DataFrame:
        if len(dataset.key_fields) == 0:
            raise ValueError(
                "Cannot add deletes to a dataset with no key fields"
            )

        if FENNEL_DELETE_TIMESTAMP in df.columns:
            return df

        return add_deletes(df, dataset.key_fields, dataset.timestamp_field)

    def _merge_df(self, df: pd.DataFrame, dataset_name: str):
        if not self.datasets[dataset_name].is_source_dataset:
            # If it's a derived dataset, just replace the data, since we
            # recompute the entire pipeline on every run.
            timestamp_field = self.datasets[
                dataset_name
            ].dataset.timestamp_field
            self.datasets[dataset_name].data = df.sort_values(timestamp_field)
        else:
            # Filter the dataframe to only include the columns in the schema
            columns = self.datasets[dataset_name].fields
            input_columns = df.columns.tolist()
            # Check that input columns are a subset of the dataset columns
            if not set(columns).issubset(set(input_columns)):
                raise ValueError(
                    f"Dataset columns {columns} are not a subset of "
                    f"Input columns {input_columns}"
                )
            # Include all hidden columns
            hidden_columns = [
                col for col in df.columns if col.startswith("__fennel")
            ]
            df = df[columns + hidden_columns]

            if isinstance(self.datasets[dataset_name].data, pd.DataFrame):
                df = pd.concat([self.datasets[dataset_name].data, df])

            # Sort by timestamp
            timestamp_field = self.datasets[
                dataset_name
            ].dataset.timestamp_field
            self.datasets[dataset_name].data = df.sort_values(timestamp_field)

    def _process_data_connector(
        self,
        dataset: Dataset,
        incremental: bool = False,
        env: Optional[str] = None,
    ) -> List[_SrcInfo]:
        def internal_webhook_present(
            srcs: List[DataSource], internal_webhook
        ) -> bool:
            for src in srcs:
                if isinstance(src, connectors.WebhookConnector):
                    if src.endpoint == internal_webhook.endpoint:
                        return True
            return False

        # Every source dataset also has a webhook source
        # with the same name as the '{dataset_name}__internal_webhook'
        internal_webhook = internal_webhook_endpoint(dataset)
        if hasattr(dataset, connectors.SOURCE_FIELD):
            srcs = getattr(dataset, connectors.SOURCE_FIELD)
        else:
            srcs = []

        if not internal_webhook_present(srcs, internal_webhook):
            srcs.append(internal_webhook)
        setattr(dataset, connectors.SOURCE_FIELD, srcs)

        sinks = getattr(dataset, connectors.SINK_FIELD, [])
        if len(sinks) > 0:
            raise ValueError(
                f"Dataset `{dataset._name}` error: Cannot define sinks on a source dataset"
            )

        sources = getattr(dataset, connectors.SOURCE_FIELD)
        sources = sources if isinstance(sources, list) else [sources]
        sources = [x for x in sources if x.envs.is_entity_selected(env)]

        webhook_sources: List[_SrcInfo] = []
        if len(sources) == 0:
            return webhook_sources
        for source in sources:
            if isinstance(source, connectors.WebhookConnector):
                src = source.data_source
                webhook_endpoint = f"{src.name}:{source.endpoint}"
                if (
                    webhook_endpoint not in self.webhook_to_dataset_map
                    or dataset._name
                    not in self.webhook_to_dataset_map[webhook_endpoint]
                ):
                    self.webhook_to_dataset_map[webhook_endpoint].append(
                        dataset._name
                    )
                webhook_sources.append(
                    _SrcInfo(
                        webhook_endpoint,
                        source.pre_proc,
                        source.bounded,
                        source.idleness,
                        datetime.now(timezone.utc),
                        source.where,
                    )
                )
        return webhook_sources

    def _cast_after_lookup(
        self, dataset_name: str, dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        """
        This helper function casts the dataframe coming from data engine having pyarrow dtype to pandas dtypes.
        Also this function would change the dtype to optional of value fields, as after lookup data_engine can
        return values or not
        """
        # Add default values.
        dtype_dict = (
            self.datasets[dataset_name].dataset.dsschema().to_dtype_proto()
        )
        for column in dataframe.columns:
            value = self.datasets[dataset_name].default_values.get(column, None)
            if pd.notna(value):
                proto_dtype = dtype_dict[column]
                if proto_dtype.HasField("decimal_type"):
                    if not isinstance(value, Decimal):
                        value = Decimal(
                            "%0.{}f".format(proto_dtype.decimal_type.scale)
                            % float(value)  # type: ignore
                        )
                elif proto_dtype.HasField("timestamp_type"):
                    value = parse_datetime(value)
                dataframe[column] = dataframe[column].fillna(value)

        # Convert non-optional fields to optional fields.
        proto_fields = (
            self.datasets[dataset_name].dataset.dsschema().to_fields_proto()
        )
        new_proto_fields = []
        key_fields = self.datasets[dataset_name].dataset.key_fields
        for proto_field in proto_fields:
            if proto_field.name not in dataframe.columns:
                continue
            if (
                proto_field.name not in [key_fields]
                and proto_field.name
                != self.datasets[dataset_name].dataset.timestamp_field
            ):
                dtype = proto_field.dtype
                if not dtype.HasField("optional_type"):
                    new_proto_fields.append(
                        schema_proto.Field(
                            name=proto_field.name,
                            dtype=schema_proto.DataType(
                                optional_type=schema_proto.OptionalType(
                                    of=dtype
                                ),
                            ),
                        )
                    )
                else:
                    new_proto_fields.append(proto_field)
            else:
                new_proto_fields.append(proto_field)
        return cast_df_to_pandas_dtype(dataframe, new_proto_fields)
