import copy
from dataclasses import dataclass
from typing import Optional, List

import pandas as pd

from fennel.datasets import Visitor, Pipeline
from fennel.lib.aggregate import Count, Sum, Max, Min, Average
from fennel.lib.duration import duration_to_timedelta


@dataclass
class NodeRet:
    df: pd.DataFrame
    timestamp_field: str
    key_fields: List[str]


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
        if obj.name not in self.data:
            return None
        return NodeRet(self.data[obj.name], obj.timestamp_field, obj.key_fields)

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
                    past_timestamp = current_timestamp - window_secs
                    filtered_df = filtered_df.loc[
                        df[input_ret.timestamp_field] >= past_timestamp
                        ]

                if isinstance(aggregate, Count):
                    # Count needs some column to aggregate on, so we use the
                    # timestamp field
                    aggs[aggregate.name] = pd.NamedAgg(
                        column=input_ret.timestamp_field, aggfunc="count"
                    )
                elif isinstance(aggregate, Sum):
                    aggs[aggregate.name] = pd.NamedAgg(
                        column=aggregate.value, aggfunc="sum"
                    )
                elif isinstance(aggregate, Average):
                    aggs[aggregate.name] = pd.NamedAgg(
                        column=aggregate.value, aggfunc="mean"
                    )
                elif isinstance(aggregate, Min):
                    aggs[aggregate.name] = pd.NamedAgg(
                        column=aggregate.value, aggfunc="min"
                    )
                elif isinstance(aggregate, Max):
                    aggs[aggregate.name] = pd.NamedAgg(
                        column=aggregate.value, aggfunc="max"
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
        if obj.on is not None and len(obj.on) > 0:
            merged_df = pd.merge_asof(
                left=left_df, right=right_df, on=left_timestamp_field, by=obj.on
            )
        else:
            merged_df = pd.merge_asof(
                left=left_df,
                right=right_df,
                on=left_timestamp_field,
                left_by=obj.left_on,
                right_by=obj.right_on,
            )
        sorted_df = merged_df.sort_values(left_timestamp_field)
        return NodeRet(sorted_df, left_timestamp_field, input_ret.key_fields)
