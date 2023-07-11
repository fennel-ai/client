import copy
import types
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from typing import Any, Optional, Dict, List

from fennel.datasets import Pipeline, Visitor, Dataset
from fennel.lib.aggregate import Count
from fennel.lib.duration import duration_to_timedelta
from fennel.lib.to_proto import Serializer
from fennel.lib.to_proto.source_code import to_includes_proto
from fennel.test_lib.execute_aggregation import get_aggregated_df

pd.set_option("display.max_columns", None)


@dataclass
class NodeRet:
    df: pd.DataFrame
    timestamp_field: str
    key_fields: List[str]
    agg_result: Optional[Dict[str, Any]] = None
    is_aggregate: bool = False


def is_subset(subset: List[str], superset: List[str]) -> bool:
    return set(subset).issubset(set(superset))


def set_match(a: List[str], b: List[str]) -> bool:
    return set(a) == set(b)


class Executor(Visitor):
    def __init__(self, data):
        super(Executor, self).__init__()
        self.dependencies = []
        self.lib_generated_code = ""
        self.data = data

    def execute(
        self, pipeline: Pipeline, dataset: Dataset
    ) -> Optional[NodeRet]:
        self.cur_pipeline_name = f"{pipeline.dataset_name}.{pipeline.name}"
        self.serializer = Serializer(pipeline, dataset)
        self.cur_ds_name = dataset._name
        return self.visit(pipeline.terminal_node)

    def visit(self, obj) -> Optional[NodeRet]:
        return super(Executor, self).visit(obj)

    def visitDataset(self, obj) -> Optional[NodeRet]:
        if obj._name not in self.data:
            return None
        return NodeRet(
            self.data[obj._name], obj.timestamp_field, obj.key_fields
        )

    def visitTransform(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        transform_func_pycode = to_includes_proto(obj.func)
        mod = types.ModuleType(obj.func.__name__)
        gen_pycode = self.serializer.wrap_function(transform_func_pycode)
        code = transform_func_pycode.imports + "\n" + gen_pycode.generated_code
        exec(code, mod.__dict__)
        func = mod.__dict__[gen_pycode.entry_point]
        try:
            t_df = func(copy.deepcopy(input_ret.df))
        except Exception as e:
            raise Exception(
                f"Error in transform function `{obj.func.__name__}` for pipeline "
                f"`{self.cur_pipeline_name}`, {e}"
            )
        num_rows = t_df.shape[0]
        if t_df is None:
            raise Exception(
                f"Transform function `{obj.func.__name__}` returned " f"None"
            )
        # Check if input_ret.df and t_df have the exactly same columns.
        input_column_names = input_ret.df.columns.values.tolist()
        output_column_names = t_df.columns.values.tolist()
        if not set_match(input_column_names, output_column_names):
            if obj.new_schema is None:
                raise ValueError(
                    f"Schema change detected in transform of pipeline "
                    f"`{self.cur_pipeline_name}`. Input columns: "
                    f"`{input_column_names}`, output columns: `{output_column_names}`"
                    ". Please provide output schema explicitly."
                )
            else:
                output_expected_column_names = obj.new_schema.keys()
                if not set_match(
                    output_expected_column_names, output_column_names
                ):
                    raise ValueError(
                        "Output schema doesnt match in transform function "
                        f"`{obj.func.__name__}` of pipeline "
                        f"{self.cur_pipeline_name}. Got output columns: "
                        f"{output_column_names}. Expected output columns:"
                        f" {output_expected_column_names}"
                    )
        if t_df.shape[0] != num_rows:
            raise ValueError(
                f"Transform function `{obj.func.__name__}` in pipeline `{self.cur_pipeline_name}` "
                f"changed number of rows from `{num_rows}` to `{t_df.shape[0]}`. To "
                f"change the number of rows, use the filter function."
            )

        sorted_df = t_df.sort_values(input_ret.timestamp_field)
        # Cast sorted_df to obj.schema()
        for col_name, col_type in obj.schema().items():
            if col_type in [float, int, str, bool]:
                sorted_df[col_name] = sorted_df[col_name].astype(col_type)

        return NodeRet(
            sorted_df, input_ret.timestamp_field, input_ret.key_fields
        )

    def visitFilter(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None

        filter_func_pycode = to_includes_proto(obj.func)
        mod = types.ModuleType(filter_func_pycode.entry_point)
        gen_pycode = self.serializer.wrap_function(
            filter_func_pycode, is_filter=True
        )
        code = gen_pycode.imports + "\n" + gen_pycode.generated_code
        exec(code, mod.__dict__)
        func = mod.__dict__[gen_pycode.entry_point]
        f_df = func(copy.deepcopy(input_ret.df))
        sorted_df = f_df.sort_values(input_ret.timestamp_field)
        return NodeRet(
            sorted_df, input_ret.timestamp_field, input_ret.key_fields
        )

    def _merge_df(
        self, df1: pd.DataFrame, df2: pd.DataFrame, ts: str
    ) -> pd.DataFrame:
        merged_df = pd.concat([df1, df2])
        return merged_df.sort_values(ts)

    def visitAggregate(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df.shape[0] == 0:
            return None
        df = copy.deepcopy(input_ret.df)
        df = df.sort_values(input_ret.timestamp_field)
        # For aggregates the result is not a dataframe but a dictionary
        # of fields to the dataframe that contains the aggregate values
        # for each timestamp for that field.
        result = {}
        for aggregate in obj.aggregates:
            # Select the columns that are needed for the aggregate
            # and drop the rest
            fields = obj.keys + [input_ret.timestamp_field]
            if not isinstance(aggregate, Count) or aggregate.unique:
                fields.append(aggregate.of)
            filtered_df = df[fields]
            result[aggregate.into_field] = get_aggregated_df(
                filtered_df, aggregate, input_ret.timestamp_field, obj.keys
            )
        return NodeRet(
            pd.DataFrame(), input_ret.timestamp_field, obj.keys, result, True
        )

    def visitJoin(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        right_ret = self.visit(obj.dataset)

        if input_ret is None or right_ret is None:
            return None

        left_df = input_ret.df
        right_df = copy.deepcopy(right_ret.df)

        if left_df.shape[0] == 0 or right_df.shape[0] == 0:
            return None

        right_timestamp_field = right_ret.timestamp_field
        left_timestamp_field = input_ret.timestamp_field
        ts_query_field = "_@@_query_ts"
        tmp_right_ts = "_@@_right_ts"
        tmp_left_ts = "_@@_left_ts"
        tmp_ts_low = "_@@_ts_low"

        # Add a column to the right dataframe that contains the timestamp which will be used for `asof_join`
        right_df[ts_query_field] = right_df[right_timestamp_field]
        # rename the right timestamp to avoid conflicts
        right_df = right_df.rename(
            columns={right_timestamp_field: tmp_right_ts}
        )

        # Set the value of the left timestamp - this is the timestamp that will be used for the join
        # - to be the upper bound of the join query (this is the max ts of a valid right dataset entry)
        def add_within_high(row):
            return row[left_timestamp_field] + duration_to_timedelta(
                obj.within[1]
            )

        left_df[ts_query_field] = left_df.apply(
            lambda row: add_within_high(row), axis=1
        )

        # Add a column to the left dataframe to specify the lower bound of the join query - this
        # is the timestamp below which we will not consider any rows from the right dataframe for joins
        def sub_within_low(row):
            if obj.within[0] == "forever":
                return datetime.min
            else:
                return row[left_timestamp_field] - duration_to_timedelta(
                    obj.within[0]
                )

        left_df[tmp_ts_low] = left_df.apply(
            lambda row: sub_within_low(row), axis=1
        )

        # rename the left timestamp to avoid conflicts
        left_df = left_df.rename(columns={left_timestamp_field: tmp_left_ts})
        if obj.on is not None and len(obj.on) > 0:
            if not is_subset(obj.on, left_df.columns):
                raise Exception(
                    f"Join on fields {obj.on} not present in left dataframe"
                )
            if not is_subset(obj.on, right_df.columns):
                raise Exception(
                    f"Join on fields {obj.on} not present in right dataframe"
                )
            # Rename the "on" columns on RHS by prefixing them with "__@@__"
            # This is to avoid conflicts with the "on" columns on LHS
            right_df = right_df.rename(
                columns={col: f"__@@__{col}" for col in obj.on}
            )
            right_by = [f"__@@__{col}" for col in obj.on]
            left_by = copy.deepcopy(obj.on)
        else:
            if not is_subset(obj.left_on, left_df.columns):
                raise Exception(
                    f"Join keys {obj.left_on} not present in left dataframe"
                )
            if not is_subset(obj.right_on, right_df.columns):
                raise Exception(
                    f"Join keys {obj.right_on} not present in right dataframe"
                )
            left_by = copy.deepcopy(obj.left_on)
            right_by = copy.deepcopy(obj.right_on)

        merged_df = pd.merge_asof(
            left=left_df,
            right=right_df,
            on=ts_query_field,
            left_by=left_by,
            right_by=right_by,
        )
        if obj.how == "inner":
            # Drop rows which have null values in any of the RHS key columns
            merged_df = merged_df.dropna(subset=right_by)
        # Drop the RHS key columns from the merged dataframe
        merged_df = merged_df.drop(columns=right_by)
        right_df = right_df.drop(columns=right_by)

        # Filter out rows that are outside the bounds of the join query
        def filter_bounded_row(row):
            # if the dataset was not found, we want to keep the row around with null values
            if pd.isnull(row[tmp_right_ts]):
                return row
            if row[tmp_right_ts] >= row[tmp_ts_low]:
                return row
            # if the row is outside the bounds, we want to assume that right join did not succeed
            # - we do so by setting all the right columns to null
            for col in right_df.columns.values:
                if col not in left_df.columns.values:
                    row[col] = np.nan
            return row

        original_dtypes = merged_df.dtypes
        # If any of the columns in the merged df was transformed to have NaN values (these are columns from RHS),
        # check if the dtype of the column is int, if so, convert that into float so that
        # it will accept NaN values - this is what Pandas does, emulating the same behavior here.
        merged_df = merged_df.transform(
            lambda row: filter_bounded_row(row), axis=1
        )
        # Try transforming to the original dtypes
        # In case of failures, ignore them, which will result in the column being converted to `object` dtype
        merged_df = merged_df.astype(original_dtypes, errors="ignore")
        transformed_dtypes = merged_df.dtypes
        for index, dtype in zip(original_dtypes.index, original_dtypes.values):
            if (
                index in right_df.columns.values
                and index not in left_df.columns.values
            ):
                if dtype == np.int64 and transformed_dtypes[index] == object:
                    original_dtypes[index] = np.dtype(np.float64)
        merged_df = merged_df.astype(original_dtypes)

        # Set the timestamp of the row to be the max of the left and right timestamps - this is the timestamp
        # that is present in the downstream operators. We take the max because it is possible that upper bound is
        # specified and join occurred with an entry with larger value than the query ts.

        def emited_ts(row):
            if pd.isnull(row[tmp_right_ts]):
                return row[tmp_left_ts]
            else:
                return max(row[tmp_right_ts], row[tmp_left_ts])

        merged_df[left_timestamp_field] = merged_df.apply(
            lambda row: emited_ts(row), axis=1
        )

        # drop the temporary columns
        merged_df.drop(
            columns=[tmp_ts_low, ts_query_field, tmp_left_ts, tmp_right_ts],
            inplace=True,
        )

        # sort the dataframe by the timestamp
        sorted_df = merged_df.sort_values(left_timestamp_field)
        return NodeRet(sorted_df, left_timestamp_field, input_ret.key_fields)

    def visitUnion(self, obj):
        dfs = [self.visit(node) for node in obj.nodes]
        if all(df is None for df in dfs):
            return None
        dfs = [df for df in dfs if df is not None]
        # Check that all the dataframes have the same timestamp field
        timestamp_fields = [df.timestamp_field for df in dfs]
        if not all(x == timestamp_fields[0] for x in timestamp_fields):
            raise Exception("Union nodes must have same timestamp field")
        # Check that all the dataframes have the same key fields
        key_fields = [df.key_fields for df in dfs]
        if not all(x == key_fields[0] for x in key_fields):
            raise Exception("Union nodes must have same key fields")
        df = pd.concat([df.df for df in dfs])
        sorted_df = df.sort_values(dfs[0].timestamp_field)
        return NodeRet(sorted_df, dfs[0].timestamp_field, dfs[0].key_fields)

    def visitRename(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        df = input_ret.df
        # Check that the columns to be renamed are present in the dataframe
        if not is_subset(obj.column_mapping.keys(), df.columns):
            raise Exception(
                f"Columns to be renamed {obj.column_mapping.keys()} not present in dataframe"
            )
        # Check that the columns post renaming are not present in the dataframe
        if is_subset(obj.column_mapping.values(), df.columns):
            raise Exception(
                f"Columns after renaming {obj.column_mapping.values()} already present in dataframe"
            )
        df = df.rename(columns=obj.column_mapping)
        for old_col, new_col in obj.column_mapping.items():
            if old_col in input_ret.key_fields:
                input_ret.key_fields.remove(old_col)
                input_ret.key_fields.append(new_col)
            if old_col == input_ret.timestamp_field:
                input_ret.timestamp_field = new_col
        return NodeRet(df, input_ret.timestamp_field, input_ret.key_fields)

    def visitDrop(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        df = input_ret.df
        df = df.drop(columns=obj.columns)
        for col in obj.columns:
            if col in input_ret.key_fields:
                input_ret.key_fields.remove(col)

        return NodeRet(df, input_ret.timestamp_field, input_ret.key_fields)

    def visitDedup(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        df = input_ret.df
        df = df.drop_duplicates(
            subset=obj.by + [input_ret.timestamp_field], keep="last"
        )
        return NodeRet(df, input_ret.timestamp_field, input_ret.key_fields)

    def visitExplode(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        df = input_ret.df
        df = df.explode(obj.columns)
        return NodeRet(df, input_ret.timestamp_field, input_ret.key_fields)
