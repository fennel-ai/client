import copy
import types
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List

import numpy as np
import pandas as pd
from fennel.expr.visitor import ExprPrinter
import pyarrow as pa
from frozendict import frozendict

import fennel.gen.schema_pb2 as schema_proto
from fennel.datasets import Pipeline, Visitor, Dataset
from fennel.datasets.datasets import DSSchema, UDFType, WINDOW_FIELD_NAME
from fennel.datasets.aggregate import (
    AggregateType,
    Average,
    Count,
    Distinct,
    ExpDecaySum,
    Sum,
    Min,
    Max,
    LastK,
    Quantile,
    Stddev,
)
from fennel.gen.schema_pb2 import Field
from fennel.internal_lib.duration import duration_to_timedelta
from fennel.internal_lib.schema import get_datatype, fennel_is_optional
from fennel.internal_lib.schema import validate_field_in_df
from fennel.internal_lib.to_proto import (
    Serializer,
    to_includes_proto,
    wrap_function,
)
from fennel.testing.execute_aggregation import (
    get_aggregated_df,
    FENNEL_DELETE_TIMESTAMP,
    add_inserts_deletes,
    FENNEL_ROW_TYPE,
)
from fennel.testing.test_utils import (
    cast_df_to_arrow_dtype,
    cast_df_to_pandas_dtype,
    add_deletes,
    get_window_data_type,
    cast_col_to_arrow_dtype,
)

pd.set_option("display.max_columns", None)


@dataclass
class NodeRet:
    df: pd.DataFrame
    timestamp_field: str
    key_fields: List[str]
    fields: List[Field]


def is_subset(subset: List[str], superset: List[str]) -> bool:
    return set(subset).issubset(set(superset))


def set_match(a: List[str], b: List[str]) -> bool:
    return set(a) == set(b)


def _primitive_type_to_pandas_dtype(dtype: Any) -> Any:
    if dtype == int:
        return pd.Int64Dtype
    elif dtype == float:
        return pd.Float64Dtype
    elif dtype == str:
        return pd.StringDtype
    elif dtype == bool:
        return pd.BooleanDtype
    return dtype


def _cast_primitive_dtype_columns(
    df: pd.DataFrame, obj: Dataset
) -> pd.DataFrame:
    pandas_dtypes = [
        pd.BooleanDtype,
        pd.Int64Dtype,
        pd.Float64Dtype,
        pd.StringDtype,
    ]
    primitive_dtypes = [int, float, bool, str]
    for col_name, col_type in obj.schema().items():
        # If col_type is Optional, then extract the actual type
        if fennel_is_optional(col_type):
            col_type = col_type.__args__[0]

        if col_type in pandas_dtypes:
            df[col_name] = df[col_name].astype(col_type())
        elif col_type in primitive_dtypes:
            df[col_name] = df[col_name].astype(
                _primitive_type_to_pandas_dtype(col_type)
            )
    return df


class Executor(Visitor):
    def __init__(self, data):
        super(Executor, self).__init__()
        self.serializer = None
        self.cur_pipeline_name = None
        self.cur_ds_name = None
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
            return NodeRet(
                None,
                obj.timestamp_field,
                obj.key_fields,
                obj.dsschema().to_fields_proto(),
            )
        return NodeRet(
            self.data[obj._name],
            obj.timestamp_field,
            obj.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitTransform(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        transform_func_pycode = to_includes_proto(obj.func)
        mod = types.ModuleType(obj.func.__name__)
        gen_pycode = wrap_function(
            self.serializer.dataset_name,
            self.serializer.dataset_code,
            self.serializer.lib_generated_code,
            transform_func_pycode,
        )
        code = transform_func_pycode.imports + "\n" + gen_pycode.generated_code
        exec(code, mod.__dict__)
        func = mod.__dict__[gen_pycode.entry_point]
        try:
            df = cast_df_to_pandas_dtype(input_ret.df, input_ret.fields)
            t_df = func(copy.deepcopy(df))
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
        # Skip hidden columns
        input_column_names = [
            col for col in input_column_names if not col.startswith("__fennel")
        ]
        output_column_names = [
            col for col in output_column_names if not col.startswith("__fennel")
        ]
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
        fields = obj.dsschema().to_fields_proto()

        # Getting schema in case there's a schema change in the transform function
        if obj.new_schema:
            fields = [
                schema_proto.Field(name=key, dtype=get_datatype(value))
                for key, value in obj.new_schema.items()
            ]
        # Cast sorted_df to new schema
        sorted_df = cast_df_to_arrow_dtype(sorted_df, fields)
        return NodeRet(
            sorted_df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            fields,
        )

    def visitFilter(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        if (
            input_ret is None
            or input_ret.df is None
            or input_ret.df.shape[0] == 0
        ):
            return None

        fields = obj.dsschema().to_fields_proto()
        if obj.filter_type == UDFType.python:
            filter_func_pycode = to_includes_proto(obj.func)
            mod = types.ModuleType(filter_func_pycode.entry_point)
            gen_pycode = wrap_function(
                self.serializer.dataset_name,
                self.serializer.dataset_code,
                self.serializer.lib_generated_code,
                filter_func_pycode,
                is_filter=True,
            )
            code = gen_pycode.imports + "\n" + gen_pycode.generated_code
            exec(code, mod.__dict__)
            func = mod.__dict__[gen_pycode.entry_point]
            try:
                df = cast_df_to_pandas_dtype(input_ret.df, fields)
                f_df = func(df).sort_values(input_ret.timestamp_field)
            except Exception as e:
                raise Exception(
                    f"Error in filter function `{obj.func.__name__}` for pipeline "
                    f"`{self.cur_pipeline_name}`, {e}"
                )
        else:
            input_df = copy.deepcopy(input_ret.df)
            f_df = input_df.copy()
            f_df.reset_index(drop=True, inplace=True)
            try:
                f_df = f_df[
                    obj.filter_expr.eval(input_df, obj.dsschema().schema())
                ]
            except Exception as e:
                printer = ExprPrinter()
                raise Exception(
                    f"Error in filter function `{printer.print(obj.filter_expr)}` for pipeline "
                    f"`{self.cur_pipeline_name}`, {e}"
                )
        sorted_df = cast_df_to_arrow_dtype(
            f_df,
            fields,
        )
        return NodeRet(
            sorted_df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def _merge_df(
        self, df1: pd.DataFrame, df2: pd.DataFrame, ts: str
    ) -> pd.DataFrame:
        merged_df = pd.concat([df1, df2])
        return merged_df.sort_values(ts)

    def visitAggregate(self, obj):
        def join_aggregated_dataset(
            schema: DSSchema,
            column_wise_df: Dict[str, pd.DataFrame],
            aggregates: List[AggregateType],
        ) -> pd.DataFrame:
            """
            Internal function to join aggregated datasets, where each aggregated
            dataset holds data for each aggregation.
            """
            key_fields = list(schema.keys.keys())
            ts_field = schema.timestamp
            required_fields = key_fields + [ts_field]
            key_dfs = pd.DataFrame()
            # Collect all timestamps across all columns
            for data in column_wise_df.values():  # type: ignore
                subset_df = data[required_fields]
                key_dfs = pd.concat([key_dfs, subset_df], ignore_index=True)
                key_dfs.drop_duplicates(inplace=True)
            # Sort key_dfs by timestamp
            key_dfs.sort_values(ts_field, inplace=True)

            # Convert key_dfs to arrow dtype
            required_fields_proto = []
            total_fields = schema.to_fields_proto()
            for field_proto in total_fields:
                if field_proto.name in required_fields:
                    required_fields_proto.append(field_proto)
            key_dfs = cast_df_to_arrow_dtype(key_dfs, required_fields_proto)

            for col in key_dfs.columns:
                # If any of the columns is a dictionary, convert it to a frozen dict
                if key_dfs[col].apply(lambda x: isinstance(x, dict)).any():
                    key_dfs[col] = key_dfs[col].apply(lambda x: frozendict(x))

            # Find the values for all columns as of the timestamp in key_dfs
            extrapolated_dfs = []
            for col, data in column_wise_df.items():  # type: ignore
                merged_df = pd.merge_asof(
                    left=key_dfs,
                    right=data,
                    on=ts_field,
                    by=key_fields,
                    direction="backward",
                    suffixes=("", "_right"),
                )
                extrapolated_dfs.append(merged_df)
            # Merge all the extrapolated dfs, column wise and drop duplicate columns
            final_df = pd.concat(extrapolated_dfs, axis=1)
            final_df = final_df.loc[:, ~final_df.columns.duplicated()]

            # Delete FENNEL_TIMESTAMP column as this should be generated again
            if FENNEL_DELETE_TIMESTAMP in df.columns:  # type: ignore
                final_df.drop(columns=[FENNEL_DELETE_TIMESTAMP], inplace=True)  # type: ignore

            # During merging there can be multiple rows which has some columns as null, we need to fill them default
            # values.
            for aggregate in aggregates:
                if isinstance(aggregate, (Count, Sum, ExpDecaySum)):
                    final_df[aggregate.into_field] = final_df[
                        aggregate.into_field
                    ].fillna(0)
                if isinstance(aggregate, (LastK, Distinct)):
                    # final_df[aggregate.into_field] = final_df[
                    #     aggregate.into_field
                    # ].fillna([])
                    # fillna doesn't work for list type or dict type :cols
                    for row in final_df.loc[
                        final_df[aggregate.into_field].isnull()
                    ].index:
                        final_df.loc[row, aggregate.into_field] = []
                if isinstance(aggregate, (Average, Min, Max, Stddev, Quantile)):
                    if pd.isna(aggregate.default):
                        final_df[aggregate.into_field] = final_df[
                            aggregate.into_field
                        ].fillna(pd.NA)
                    else:
                        final_df[aggregate.into_field] = final_df[
                            aggregate.into_field
                        ].fillna(aggregate.default)

            return final_df

        input_ret = self.visit(obj.node)
        if (
            input_ret is None
            or input_ret.df is None
            or input_ret.df.shape[0] == 0
        ):
            return None
        df = copy.deepcopy(input_ret.df)
        if obj.along is not None:
            df[input_ret.timestamp_field] = df[obj.along]
        df = df.sort_values(input_ret.timestamp_field)
        # For aggregates the result is not a dataframe but a dictionary
        # of fields to the dataframe that contains the aggregate values
        # for each timestamp for that field.
        result = {}
        output_schema = obj.dsschema()
        if len(obj.aggregates) > 0:
            for aggregate in obj.aggregates:
                # Select the columns that are needed for the aggregate -> all keys without window
                # and drop the rest
                fields = obj.keys_without_window + [input_ret.timestamp_field]
                if not isinstance(aggregate, Count) or aggregate.unique:
                    fields.append(aggregate.of)
                if FENNEL_DELETE_TIMESTAMP in df.columns:
                    filtered_df = df[fields + [FENNEL_DELETE_TIMESTAMP]]
                else:
                    filtered_df = df[fields]

                result[aggregate.into_field] = get_aggregated_df(
                    filtered_df,
                    aggregate,
                    input_ret.timestamp_field,
                    obj.keys_without_window.copy(),
                    output_schema.values[aggregate.into_field],
                    True if obj.window_field else False,
                )
            final_df = join_aggregated_dataset(
                output_schema, result, obj.aggregates
            )
        else:
            # this is the case where 'window' param in groupby is used
            if not obj.window_field:
                raise ValueError(
                    "Cannot aggregate without any aggregate specs."
                )

            fields = obj.keys_without_window + [input_ret.timestamp_field]
            if FENNEL_DELETE_TIMESTAMP in df.columns:
                filtered_df = df[fields + [FENNEL_DELETE_TIMESTAMP]]
            else:
                filtered_df = df[fields]

            filtered_df[FENNEL_ROW_TYPE] = 1

            final_df = add_inserts_deletes(
                filtered_df,
                obj.window_field,
                obj.keys_without_window,
                input_ret.timestamp_field,
                True,
            )

            # Select only non deleted rows:
            final_df = final_df.loc[final_df[FENNEL_ROW_TYPE] == 1].reset_index(
                drop=True
            )
            subset = obj.keys + [input_ret.timestamp_field]

            # Get the last value against each group
            final_df = (
                final_df.drop_duplicates(subset=subset, keep="last")
                .reset_index(drop=True)
                .loc[:, subset]
            )

        if obj.window_field:
            # Convert window to pyarrow dtype
            final_df[WINDOW_FIELD_NAME] = cast_col_to_arrow_dtype(
                final_df[WINDOW_FIELD_NAME], get_window_data_type()
            )

        return NodeRet(
            final_df,
            input_ret.timestamp_field,
            obj.keys,
            output_schema.to_fields_proto(),
        )

    def visitJoin(self, obj) -> Optional[NodeRet]:
        input_ret = self.visit(obj.node)
        right_ret = self.visit(obj.dataset)
        if input_ret is None or input_ret.df is None:
            return None

        left_df = input_ret.df

        if left_df is None or left_df.shape[0] == 0:
            return None

        if (
            right_ret is None
            or right_ret.df is None
            or right_ret.df.shape[0] == 0
        ):
            if obj.how == "inner":
                return None
            else:
                right_df = pd.DataFrame(
                    columns=[f.name for f in right_ret.fields]  # type: ignore
                    + [FENNEL_DELETE_TIMESTAMP]
                )
                right_df = cast_df_to_arrow_dtype(right_df, right_ret.fields)  # type: ignore
        else:
            right_df = copy.deepcopy(right_ret.df)

        right_timestamp_field = right_ret.timestamp_field  # type: ignore
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
        # Conserve the rhs timestamp field if the join demands it
        if (
            obj.fields is not None
            and len(obj.fields) > 0
            and right_timestamp_field in obj.fields
        ):
            right_df[right_timestamp_field] = right_df[tmp_right_ts]

        # Set the value of the left timestamp - this is the timestamp that will be used for the join
        # - to be the upper bound of the join query (this is the max ts of a valid right dataset entry)
        def add_within_high(row):
            return row[left_timestamp_field] + duration_to_timedelta(
                obj.within[1]
            )

        left_df[ts_query_field] = left_df.apply(
            lambda row: add_within_high(row), axis=1
        ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))

        # Add a column to the left dataframe to specify the lower bound of the join query - this
        # is the timestamp below which we will not consider any rows from the right dataframe for joins
        def sub_within_low(row):
            if obj.within[0] == "forever":
                # datetime.min cannot be casted to nano seconds therefore using
                # 1701-01-01 00:00:00.000000+00:0 as minimum datetime
                return datetime(1700, 1, 1, tzinfo=timezone.utc)
            else:
                return row[left_timestamp_field] - duration_to_timedelta(
                    obj.within[0]
                )

        left_df[tmp_ts_low] = left_df.apply(
            lambda row: sub_within_low(row), axis=1
        ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))

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

        # Rename the right_by columns to avoid conflicts with any of the left columns.
        # We dont need to worry about right value columns conflicting with left key columns,
        # because we have already verified that.
        if set(right_by).intersection(set(left_df.columns)):
            right_df = right_df.rename(
                columns={col: f"__@@__{col}" for col in right_by}
            )
            right_by = [f"__@@__{col}" for col in right_by]

        left_df = left_df.sort_values(by=ts_query_field)
        right_df = right_df.sort_values(by=ts_query_field)

        for col in left_by:
            # If any of the columns is a dictionary, convert it to a frozen dict
            if left_df[col].apply(lambda x: isinstance(x, dict)).any():
                left_df[col] = left_df[col].apply(lambda x: frozendict(x))

        try:
            # TODO(Nitin): Use FENNEL_DELETE_TIMESTAMP to filter out deleted rows appropriately
            right_df = right_df.drop(columns=[FENNEL_DELETE_TIMESTAMP])
            merged_df = pd.merge_asof(
                left=left_df,
                right=right_df,
                on=ts_query_field,
                left_by=left_by,
                right_by=right_by,
            )
        except Exception as e:
            raise Exception(
                f"Error in join function for pipeline "
                f"`{self.cur_pipeline_name}` in dataset `{self.cur_ds_name}, {e}"
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
                    row[col] = pd.NA
            return row

        original_dtypes = merged_df.dtypes
        # If any of the columns in the merged df was transformed to have NaN values (these are columns from RHS),
        # check if the dtype of the column is int, if so, convert that into float so that
        # it will accept NaN values - this is what Pandas does, emulating the same behavior here.
        # Handling with empty dataframe case because pandas automatically defines dtype of float64 to null columns
        # and then conversion of flot64 dtype to pd.ArrowDtype(pa.timestamp("us", "UTC")) fails
        if merged_df.shape[0] > 0:
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

        # Handling with empty dataframe case because pandas automatically defines dtype of float64 to null columns
        # and then conversion of flot64 dtype to pd.ArrowDtype(pa.timestamp("us", "UTC")) fails
        if merged_df.shape[0] > 0:
            merged_df[left_timestamp_field] = merged_df.apply(
                lambda row: emited_ts(row), axis=1
            )
        else:
            merged_df[left_timestamp_field] = pd.Series(
                [], dtype=pd.ArrowDtype(pa.timestamp("ns", "UTC"))
            )

        # drop the temporary columns
        merged_df.drop(
            columns=[tmp_ts_low, ts_query_field, tmp_left_ts, tmp_right_ts],
            inplace=True,
        )

        if obj.fields is not None and len(obj.fields) > 0:
            all_right_fields = [f.name for f in right_ret.fields]  # type: ignore
            for col_name in obj.fields:
                if col_name in right_ret.key_fields:  # type: ignore
                    raise Exception(
                        f"fields member {col_name} cannot be one of right dataframe's "
                        f"key fields {right_ret.key_fields}"  # type: ignore
                    )
                if col_name not in all_right_fields:
                    raise Exception(
                        f"fields member {col_name} not present in right dataframe's "  # type: ignore
                        f"fields {right_ret.fields}"
                    )

            cols_to_drop = []
            for field in right_ret.fields:  # type: ignore
                col_name = field.name
                if (
                    col_name not in right_ret.key_fields  # type: ignore
                    and col_name != right_ret.timestamp_field  # type: ignore
                    and col_name not in obj.fields
                ):
                    cols_to_drop.append(col_name)

            merged_df.drop(columns=cols_to_drop, inplace=True)

        # sort the dataframe by the timestamp
        sorted_df = merged_df.sort_values(left_timestamp_field)

        # Cast the joined dataframe to arrow dtype
        fields = obj.dsschema().to_fields_proto()
        sorted_df = cast_df_to_arrow_dtype(sorted_df, fields)

        return NodeRet(
            sorted_df,
            left_timestamp_field,
            input_ret.key_fields,
            fields,
        )

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
        return NodeRet(
            sorted_df,
            dfs[0].timestamp_field,
            dfs[0].key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitRename(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
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
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitDrop(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        df = input_ret.df
        df = df.drop(columns=obj.columns)
        for col in obj.columns:
            if col in input_ret.key_fields:
                input_ret.key_fields.remove(col)

        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitSelect(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        df = input_ret.df
        df = df.drop(columns=obj.drop_columns)
        for col in obj.drop_columns:
            if col in input_ret.key_fields:
                input_ret.key_fields.remove(col)

        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitDropNull(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        df = input_ret.df
        df = df.dropna(subset=obj.columns)
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitAssign(self, obj):
        input_ret = self.visit(obj.node)
        if (
            input_ret is None
            or input_ret.df is None
            or input_ret.df.shape[0] == 0
        ):
            return None

        fields = obj.dsschema().to_fields_proto()
        if obj.assign_type == UDFType.python:
            assign_func_pycode = to_includes_proto(obj.func)
            mod = types.ModuleType(assign_func_pycode.entry_point)
            gen_pycode = wrap_function(
                self.serializer.dataset_name,
                self.serializer.dataset_code,
                self.serializer.lib_generated_code,
                assign_func_pycode,
                is_assign=True,
                column_name=obj.column,
            )
            code = gen_pycode.imports + "\n" + gen_pycode.generated_code
            exec(code, mod.__dict__)
            func = mod.__dict__[gen_pycode.entry_point]
            try:
                df = cast_df_to_pandas_dtype(input_ret.df, input_ret.fields)
                df = func(df)
                field = schema_proto.Field(
                    name=obj.column, dtype=get_datatype(obj.output_type)
                )

                # Check the schema of the column
                validate_field_in_df(field, df, self.cur_pipeline_name)
            except Exception as e:
                raise Exception(
                    f"Error in assign node for column `{obj.column}` for pipeline "
                    f"`{self.cur_pipeline_name}`, {e}"
                )
        else:
            input_df = copy.deepcopy(input_ret.df)
            df = copy.deepcopy(input_df)
            df.reset_index(drop=True, inplace=True)
            for col, typed_expr in obj.output_expressions.items():
                input_dsschema = obj.node.dsschema().schema()
                try:
                    df[col] = typed_expr.expr.eval(
                        input_df, input_dsschema, typed_expr.dtype, parse=False
                    )
                except Exception as e:
                    raise Exception(
                        f"Error in assign node for column `{col}` for pipeline "
                        f"`{self.cur_pipeline_name}`, {e}"
                    )
        # Cast to arrow dtype
        df = cast_df_to_arrow_dtype(df, fields)
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            fields,
        )

    def visitDedup(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        df = input_ret.df
        df = df.drop_duplicates(
            subset=obj.by + [input_ret.timestamp_field], keep="last"
        )
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitExplode(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        df = input_ret.df
        # Ignore the index when exploding the dataframe. This is because the index is not unique after exploding and
        # we should reset it. This is the behavior in our engine (where we don't have an index) and the operations
        # on the dataframe are not affected/associated by the index.
        df = df.explode(obj.columns, ignore_index=True)

        # Cast exploded column to right arrow dtype
        fields = obj.dsschema().to_fields_proto()
        df = cast_df_to_arrow_dtype(df, fields)
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            fields,
        )

    def visitFirst(self, obj):
        input_ret = self.visit(obj.node)
        if (
            input_ret is None
            or input_ret.df is None
            or input_ret.df.shape[0] == 0
        ):
            return None
        df = copy.deepcopy(input_ret.df)
        df = df.sort_values(input_ret.timestamp_field)
        df = df.groupby(obj.keys).first().reset_index()
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitLatest(self, obj):
        input_ret = self.visit(obj.node)
        if (
            input_ret is None
            or input_ret.df is None
            or input_ret.df.shape[0] == 0
        ):
            return None
        df = copy.deepcopy(input_ret.df)
        df = df.sort_values(input_ret.timestamp_field)
        df = add_deletes(df, obj.keys, input_ret.timestamp_field)
        return NodeRet(
            df,
            input_ret.timestamp_field,
            input_ret.key_fields,
            obj.dsschema().to_fields_proto(),
        )

    def visitChangelog(self, obj):
        input_ret = self.visit(obj.node)
        if input_ret is None or input_ret.df is None:
            return None
        if FENNEL_DELETE_TIMESTAMP not in input_ret.df.columns:
            raise Exception(
                "Changelog node can only be applied to a keyed dataset"
            )
        df = input_ret.df
        delete_df = df[df[FENNEL_DELETE_TIMESTAMP].notnull()]
        delete_df[input_ret.timestamp_field] = delete_df[
            FENNEL_DELETE_TIMESTAMP
        ]
        df[obj.delete_column] = False
        delete_df[obj.delete_column] = True
        df = pd.concat([df, delete_df])
        df = df.drop(columns=[FENNEL_DELETE_TIMESTAMP])
        # Sort the dataframe by timestamp
        df = df.sort_values(input_ret.timestamp_field)
        return NodeRet(
            df,
            input_ret.timestamp_field,
            [],
            obj.dsschema().to_fields_proto(),
        )
