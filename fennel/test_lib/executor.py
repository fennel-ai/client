import copy
from dataclasses import dataclass
from typing import Optional, List

import numpy as np
import pandas as pd

from fennel.datasets import Visitor, Pipeline
from fennel.lib.aggregate import Count, Sum, Max, Min, Average
from fennel.lib.duration import duration_to_timedelta


@dataclass
class NodeRet:
    df: pd.DataFrame
    timestamp_field: str
    key_fields: List[str]


def is_subset(subset: List[str], superset: List[str]) -> bool:
    return set(subset).issubset(set(superset))


class Executor(Visitor):
    def __init__(self, data):
        super(Executor, self).__init__()
        self.data = data

    def execute(self, pipeline: Pipeline) -> Optional[NodeRet]:
        return self.visit(pipeline.node)

    def visit(self, obj) -> Optional[NodeRet]:
        ret = super(Executor, self).visit(obj)
        return ret

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
        t_df = obj.func(copy.deepcopy(input_ret.df))
        sorted_df = t_df.sort_values(input_ret.timestamp_field)
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
        timestamped_dfs = []
        for current_timestamp in timestamps:
            aggs = {}
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
            timestamped_dfs.append(agg_df)
        return NodeRet(
            pd.concat(timestamped_dfs), input_ret.timestamp_field, obj.keys
        )

    def visitJoin(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        right_ret = self.visit(obj.dataset)
        if input_ret is None or right_ret is None:
            return None

        left_df = input_ret.df
        right_df = copy.deepcopy(right_ret.df)

        right_timestamp_field = input_ret.timestamp_field
        left_timestamp_field = right_ret.timestamp_field
        right_df = right_df.rename(
            columns={right_timestamp_field: left_timestamp_field}
        )
        pd.set_option("display.max_columns", None)
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
