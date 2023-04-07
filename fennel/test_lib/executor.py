import copy
import types
from dataclasses import dataclass

import pandas as pd
from typing import Any, Optional, Dict, List

from fennel.datasets import Pipeline, Visitor, Dataset
from fennel.lib.aggregate import Count
from fennel.lib.to_proto import Serializer
from fennel.lib.to_proto.source_code import to_includes_proto
from fennel.test_lib.execute_aggregation import get_aggregated_df


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
    def __init__(self, data, agg_state):
        super(Executor, self).__init__()
        self.dependencies = []
        self.lib_generated_code = ""
        self.data = data
        self.agg_state = agg_state

    def execute(
        self, pipeline: Pipeline, dataset: Dataset
    ) -> Optional[NodeRet]:
        self.cur_pipeline_name = pipeline.name
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
        if input_ret is None:
            return None
        df = copy.deepcopy(input_ret.df)

        if len(input_ret.key_fields) > 0:
            # Pick the latest value for each key
            df = df.groupby(input_ret.key_fields).apply(
                lambda x: x.sort_values(input_ret.timestamp_field).iloc[-1]
            )
            df = df.reset_index(drop=True)
        df = df.sort_values(input_ret.timestamp_field)
        if self.cur_ds_name in self.agg_state:
            # Merge the current dataframe with the previous state
            df = self._merge_df(
                self.agg_state[self.cur_ds_name], df, input_ret.timestamp_field
            )
            self.agg_state[self.cur_ds_name] = df
        else:
            self.agg_state[self.cur_ds_name] = df
        # For aggregates the result is not a dataframe but a dictionary
        # of fields to the dataframe that contains the aggregate values
        # for each timestamp for that field.
        result = {}
        for aggregate in obj.aggregates:
            # Select the columns that are needed for the aggregate
            # and drop the rest
            fields = obj.keys + [input_ret.timestamp_field]
            if not isinstance(aggregate, Count):
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
