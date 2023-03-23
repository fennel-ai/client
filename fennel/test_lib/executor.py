import copy
from dataclasses import dataclass
from functools import reduce
from typing import Optional, List

import numpy as np
import pandas as pd

from fennel.datasets import Pipeline, Visitor
from fennel.lib.aggregate import Count, Sum, Max, Min, Average
from fennel.lib.duration import duration_to_timedelta


@dataclass
class NodeRet:
    df: pd.DataFrame
    timestamp_field: str
    key_fields: List[str]


def is_subset(subset: List[str], superset: List[str]) -> bool:
    return set(subset).issubset(set(superset))


def set_match(a: List[str], b: List[str]) -> bool:
    return set(a) == set(b)


class Executor(Visitor):
    def __init__(self, data):
        super(Executor, self).__init__()
        self.data = data

    def execute(self, pipeline: Pipeline) -> Optional[NodeRet]:
        self.cur_pipeline_name = pipeline.name
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
        try:
            t_df = obj.func(copy.deepcopy(input_ret.df))
        except Exception as e:
            raise Exception(
                f"Error in transform function for pipeline "
                f"{self.cur_pipeline_name}, {e}"
            )
        if t_df is None:
            raise Exception(
                f"Transform function {obj.func.__name__} returned " f"None"
            )
        # Check if input_ret.df and t_df have the exactly same columns.
        input_column_names = input_ret.df.columns.values.tolist()
        output_column_names = t_df.columns.values.tolist()
        if not set_match(input_column_names, output_column_names):
            if obj.schema is None:
                raise ValueError(
                    f"Schema change detected in transform of pipeline "
                    f"{self.cur_pipeline_name}. Input columns: "
                    f"{input_column_names}, output columns: {output_column_names}"
                    ". Please provide output schema explicitly."
                )
            else:
                output_expected_column_names = obj.schema.keys()
                if not set_match(
                    output_expected_column_names, output_column_names
                ):
                    raise ValueError(
                        "Output schema doesnt match in transform of pipeline "
                        f"{self.cur_pipeline_name}. Got output columns: "
                        f"{output_column_names}. Expected output columns:"
                        f" {output_expected_column_names}"
                    )

        sorted_df = t_df.sort_values(input_ret.timestamp_field)
        return NodeRet(
            sorted_df, input_ret.timestamp_field, input_ret.key_fields
        )

    def visitFilter(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        filter_bool = obj.func(copy.deepcopy(input_ret.df))
        if type(filter_bool) != pd.Series:
            raise Exception(
                f"Filter function {obj.func.__name__} returned "
                f"non-boolean array"
            )
        f_df = input_ret.df[filter_bool]
        sorted_df = f_df.sort_values(input_ret.timestamp_field)
        return NodeRet(
            sorted_df, input_ret.timestamp_field, input_ret.key_fields
        )

    def visitAggregate(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        if input_ret is None:
            return None
        df = copy.deepcopy(input_ret.df)
        if len(input_ret.key_fields) > 0:
            # Pick the latest value for each key
            df = df.groupby(input_ret.key_fields).apply(
                lambda x: x.sort_values(input_ret.timestamp_field).iloc[-1]
            )
            df = df.reset_index(drop=True)
        # Run an aggregate for each timestamp in the dataframe
        # So that appropriate windows can be applied and correct
        # timestamps can be assigned
        df = df.sort_values(input_ret.timestamp_field)
        timestamps = df[input_ret.timestamp_field].unique()
        timestamps_with_expired_events = copy.deepcopy(timestamps)
        for current_timestamp in timestamps:
            for aggregate in obj.aggregates:
                if aggregate.window.start != "forever":
                    window_secs = duration_to_timedelta(
                        aggregate.window.start
                    ).total_seconds()
                    expired_timestamp = current_timestamp + np.timedelta64(
                        int(window_secs + 1), "s"
                    )
                    timestamps_with_expired_events = np.append(
                        timestamps_with_expired_events, expired_timestamp
                    )
        timestamps_with_expired_events.sort()
        timestamps_with_expired_events = np.unique(
            timestamps_with_expired_events
        )
        timestamped_dfs = []
        for current_timestamp in timestamps_with_expired_events:
            aggs = {}
            agg_dfs = []
            for aggregate in obj.aggregates:
                # Run the aggregate for all data upto the current row
                # for the given window and assign it the timestamp of the row.
                filtered_df = copy.deepcopy(
                    df[df[input_ret.timestamp_field] <= current_timestamp]
                )
                if aggregate.window.start != "forever":
                    window_secs = duration_to_timedelta(
                        aggregate.window.start
                    ).total_seconds()
                    past_timestamp = current_timestamp - np.timedelta64(
                        int(window_secs), "s"
                    )
                    select_rows = filtered_df[input_ret.timestamp_field] >= (
                        past_timestamp
                    )
                    filtered_df = filtered_df.loc[select_rows]

                if isinstance(aggregate, Count):
                    # Count needs some column to aggregate on, so we use the
                    # timestamp field
                    aggs[aggregate.into_field] = pd.NamedAgg(
                        column=input_ret.timestamp_field, aggfunc="count"
                    )
                elif isinstance(aggregate, Sum):
                    aggs[aggregate.into_field] = pd.NamedAgg(
                        column=aggregate.of, aggfunc="sum"
                    )
                elif isinstance(aggregate, Average):
                    aggs[aggregate.into_field] = pd.NamedAgg(
                        column=aggregate.of, aggfunc="mean"
                    )
                elif isinstance(aggregate, Min):
                    aggs[aggregate.into_field] = pd.NamedAgg(
                        column=aggregate.of, aggfunc="min"
                    )
                elif isinstance(aggregate, Max):
                    aggs[aggregate.into_field] = pd.NamedAgg(
                        column=aggregate.of, aggfunc="max"
                    )
                agg_df = filtered_df.groupby(obj.keys).agg(**aggs).reset_index()
                agg_df[input_ret.timestamp_field] = current_timestamp
                agg_dfs.append(agg_df)
                aggs = {}

            join_columns = obj.keys + [input_ret.timestamp_field]
            df_merged = reduce(
                lambda left, right: pd.merge(
                    left, right, on=join_columns, how="outer"
                ),
                agg_dfs,
            )
            timestamped_dfs.append(df_merged)
        post_aggregated_data = pd.concat(timestamped_dfs)
        post_aggregated_data = post_aggregated_data.reset_index(drop=True)
        post_aggregated_data.fillna(0, inplace=True)
        post_aggregated_data = post_aggregated_data.convert_dtypes(
            convert_string=False
        )

        for col in post_aggregated_data.columns:
            # Cast columns to numpy dtypes from pandas types
            if isinstance(post_aggregated_data[col].dtype, pd.Int64Dtype):
                post_aggregated_data[col] = post_aggregated_data[col].astype(
                    np.int64
                )
            elif isinstance(post_aggregated_data[col].dtype, pd.Float64Dtype):
                post_aggregated_data[col] = post_aggregated_data[col].astype(
                    np.float64
                )
            elif isinstance(post_aggregated_data[col].dtype, pd.BooleanDtype):
                post_aggregated_data[col] = post_aggregated_data[col].astype(
                    np.bool_
                )

        return NodeRet(
            post_aggregated_data, input_ret.timestamp_field, obj.keys
        )

    def visitJoin(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        right_ret = self.visit(obj.dataset)
        if input_ret is None or right_ret is None:
            return None

        left_df = input_ret.df
        right_df = copy.deepcopy(right_ret.df)

        right_timestamp_field = right_ret.timestamp_field
        left_timestamp_field = input_ret.timestamp_field
        right_df = right_df.rename(
            columns={right_timestamp_field: left_timestamp_field}
        )
        if obj.on is not None and len(obj.on) > 0:
            if not is_subset(obj.on, left_df.columns):
                raise Exception(
                    f"Join on fields {obj.on} not present in left dataframe"
                )
            if not is_subset(obj.on, right_df.columns):
                raise Exception(
                    f"Join on fields {obj.on} not present in right dataframe"
                )
            merged_df = pd.merge_asof(
                left=left_df, right=right_df, on=left_timestamp_field, by=obj.on
            )
        else:
            if not is_subset(obj.left_on, left_df.columns):
                raise Exception(
                    f"Join keys {obj.left_on} not present in left dataframe"
                )
            if not is_subset(obj.right_on, right_df.columns):
                raise Exception(
                    f"Join keys {obj.right_on} not present in right dataframe"
                )
            merged_df = pd.merge_asof(
                left=left_df,
                right=right_df,
                on=left_timestamp_field,
                left_by=obj.left_on,
                right_by=obj.right_on,
            )
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
