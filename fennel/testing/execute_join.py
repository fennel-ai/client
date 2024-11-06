import pandas as pd
from typing import List, Optional
from fennel.testing.test_utils import FENNEL_DELETE_TIMESTAMP, FENNEL_LOOKUP
import bisect
from fennel.testing.executor import NodeRet, is_subset
from fennel.internal_lib.duration.duration import duration_to_timedelta
from datetime import datetime, timezone
import pyarrow as pa
import numpy as np
import copy
from typing import Tuple
from fennel.testing.test_utils import cast_df_to_arrow_dtype
from frozendict import frozendict


def min_ts_with_nat(a, b):
    if a is pd.NaT or a is None:
        return b
    if b is pd.NaT or b is None:
        return a
    return min(a, b)


def left_join_empty(
    left_node: NodeRet,
    right_node: Optional[NodeRet],
    right_value_schema: List[str],
    right_fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    result = []
    if right_fields is None:
        right_fields = right_value_schema
    for _, left_row in left_node.df.iterrows():
        deleted_timestamp = (
            left_row[FENNEL_DELETE_TIMESTAMP]
            if FENNEL_DELETE_TIMESTAMP in left_row
            else pd.NaT
        )
        result.append(
            _create_null_row(
                left_row,
                left_node,
                right_node,
                left_row[left_node.timestamp_field],
                deleted_timestamp,
                right_fields,
            )
        )
    result_df = pd.DataFrame(result)
    result_df = result_df.drop(columns=[FENNEL_LOOKUP])
    if len(left_node.key_fields) == 0:
        result_df = result_df.drop(columns=[FENNEL_DELETE_TIMESTAMP])
    return result_df


def _combine_rows(
    left_row,
    right_row,
    left_node: NodeRet,
    right_node: NodeRet,
    right_fields: Optional[List[str]] = None,
):
    insert_timestamp = max(
        left_row[left_node.timestamp_field],
        right_row[right_node.timestamp_field],
    )
    delete_timestamp = min_ts_with_nat(
        left_row[FENNEL_DELETE_TIMESTAMP], right_row[FENNEL_DELETE_TIMESTAMP]
    )
    # Generate result tuple
    # We add all fields from LHS and the chosen fields from RHS
    result_tuple = dict()
    for key in left_node.fields:
        result_tuple[key.name] = left_row[key.name]
    right_fields = right_fields or [
        x.name
        for x in right_node.fields
        if x.name not in right_node.key_fields
        and x.name != right_node.timestamp_field
    ]
    for field in right_fields:
        result_tuple[field] = right_row[field]

    # Add insert and delete timestamps
    # Insert timestamp is the max of the two timestamps
    # Delete timestamp is the min of the two timestamps
    result_tuple[left_node.timestamp_field] = insert_timestamp
    result_tuple[FENNEL_DELETE_TIMESTAMP] = delete_timestamp
    result_tuple[FENNEL_LOOKUP] = True
    return result_tuple


def _create_null_row(
    left_row,
    left_node: NodeRet,
    right_node: Optional[NodeRet],
    timestamp: datetime,
    deleted_timestamp: datetime,
    right_fields: Optional[List[str]] = None,
):
    result_tuple = dict()
    left_fields = left_node.key_fields + [x.name for x in left_node.fields]
    for key in left_fields:
        result_tuple[key] = left_row[key]
    result_tuple[left_node.timestamp_field] = timestamp
    result_tuple[FENNEL_DELETE_TIMESTAMP] = deleted_timestamp
    if right_fields is None:
        if right_node is None:
            right_fields = []
        else:
            right_fields = right_fields or [
                x.name
                for x in right_node.fields
                if x.name not in right_node.key_fields
                and x.name != right_node.timestamp_field
            ]
    for field in right_fields:
        result_tuple[field] = None
    result_tuple[FENNEL_LOOKUP] = False
    return result_tuple


def _validate_join_fields(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    right_ret: NodeRet,
    right_key_fields: List[str],
    on: Optional[List[str]] = None,
    left_on: Optional[List[str]] = None,
    right_on: Optional[List[str]] = None,
    right_fields: Optional[List[str]] = None,
):
    if on is not None and len(on) > 0:
        if not is_subset(on, left_df.columns):
            raise Exception(
                f"Join on fields {on} not present in left dataframe"
            )
        if not is_subset(on, right_df.columns):
            raise Exception(
                f"Join on fields {on} not present in right dataframe"
            )
    else:
        if left_on is None:
            raise Exception("left_on is required if on is not provided")
        if not is_subset(left_on, left_df.columns):
            raise Exception(
                f"Join keys {left_on} not present in left dataframe"
            )
        if right_on is None:
            raise Exception("right_on is required if on is not provided")
        if not is_subset(right_on, right_df.columns):
            raise Exception(
                f"Join keys {right_on} not present in right dataframe"
            )

    if right_fields is not None and len(right_fields) > 0:
        all_right_fields = [f for f in right_fields]  # type: ignore
        for col_name in right_fields:
            if col_name in right_ret.key_fields:  # type: ignore
                raise Exception(
                    f"fields member {col_name} cannot be one of right dataframe's "  # type: ignore
                    f"key fields {right_key_fields}"
                )
            if col_name not in all_right_fields:
                raise Exception(
                    f"fields member {col_name} not present in right dataframe's "  # type: ignore
                    f"fields {right_ret.fields}"  # type: ignore
                )


# Perform stream table join between an append only stream dataframe and a upsert table dataframe
def stream_table_join(
    input_ret: NodeRet,
    right_ret: NodeRet,
    how: str,
    within: Tuple[str, str],
    on: Optional[List[str]] = None,
    left_on: Optional[List[str]] = None,
    right_on: Optional[List[str]] = None,
    right_fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    left_df = input_ret.df
    if right_ret is None or right_ret.df is None or right_ret.df.shape[0] == 0:
        right_df = pd.DataFrame(
            columns=[f.name for f in right_ret.fields]  # type: ignore
            + [FENNEL_DELETE_TIMESTAMP]
        )
        right_df = cast_df_to_arrow_dtype(right_df, right_ret.fields)  # type: ignore
    else:
        right_df = copy.deepcopy(right_ret.df)

    _validate_join_fields(
        left_df,
        right_df,
        right_ret,
        right_ret.key_fields,
        on,
        left_on,
        right_on,
        right_fields,
    )

    right_timestamp_field = right_ret.timestamp_field  # type: ignore
    left_timestamp_field = input_ret.timestamp_field
    ts_query_field = "_@@_query_ts"
    tmp_right_ts = "_@@_right_ts"
    tmp_left_ts = "_@@_left_ts"
    tmp_ts_low = "_@@_ts_low"

    # Add a column to the right dataframe that contains the timestamp which will be used for `asof_join`
    right_df[ts_query_field] = right_df[right_timestamp_field]
    # rename the right timestamp to avoid conflicts
    right_df = right_df.rename(columns={right_timestamp_field: tmp_right_ts})
    # Conserve the rhs timestamp field if the join demands it
    if (
        right_fields is not None
        and len(right_fields) > 0
        and right_timestamp_field in right_fields
    ):
        right_df[right_timestamp_field] = right_df[tmp_right_ts]

    # Set the value of the left timestamp - this is the timestamp that will be used for the join
    # - to be the upper bound of the join query (this is the max ts of a valid right dataset entry)
    def add_within_high(row):
        return row[left_timestamp_field] + duration_to_timedelta(within[1])

    left_df[ts_query_field] = left_df.apply(
        lambda row: add_within_high(row), axis=1
    ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))

    # Add a column to the left dataframe to specify the lower bound of the join query - this
    # is the timestamp below which we will not consider any rows from the right dataframe for joins
    def sub_within_low(row):
        if within[0] == "forever":
            # datetime.min cannot be casted to nano seconds therefore using
            # 1701-01-01 00:00:00.000000+00:0 as minimum datetime
            return datetime(1700, 1, 1, tzinfo=timezone.utc)
        else:
            return row[left_timestamp_field] - duration_to_timedelta(within[0])

    left_df[tmp_ts_low] = left_df.apply(
        lambda row: sub_within_low(row), axis=1
    ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))

    # rename the left timestamp to avoid conflicts
    left_df = left_df.rename(columns={left_timestamp_field: tmp_left_ts})
    if on is not None and len(on) > 0:
        # Rename the "on" columns on RHS by prefixing them with "__@@__"
        # This is to avoid conflicts with the "on" columns on LHS
        right_df = right_df.rename(columns={col: f"__@@__{col}" for col in on})
        right_by = [f"__@@__{col}" for col in on]
        left_by = copy.deepcopy(on)
    else:
        left_by = copy.deepcopy(left_on)  # type: ignore
        right_by = copy.deepcopy(right_on)  # type: ignore

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

    # Drop fields from right which are not in fields
    if right_fields is not None and len(right_fields) > 0:
        cols_to_drop = []
        for field in right_ret.fields:  # type: ignore
            col_name = field.name
            if (
                col_name not in right_ret.key_fields  # type: ignore
                and col_name != right_ret.timestamp_field  # type: ignore
                and col_name not in right_fields
            ):
                cols_to_drop.append(col_name)

        right_df.drop(columns=cols_to_drop, inplace=True)

    right_df = right_df.drop(columns=[FENNEL_DELETE_TIMESTAMP])
    merged_df = pd.merge_asof(
        left=left_df,
        right=right_df,
        on=ts_query_field,
        left_by=left_by,
        right_by=right_by,
    )
    if how == "inner":
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
        return row[tmp_left_ts]

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

    return merged_df


# Perform table table join between two upsert table dataframes
def table_table_join(
    left_node: NodeRet,
    right_node: NodeRet,
    how: str,
    on: Optional[List[str]] = None,
    left_on: Optional[List[str]] = None,
    right_on: Optional[List[str]] = None,
    right_fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Perform left table join between two upsert stream dataframes.
    Args:
        left_node: Left node
        right_node: Right node
        left_join_cols: Columns to join on from left dataframe
        within: Within range for the join query
    Returns:
        Joined dataframe as an upsert stream
    """
    left_df = left_node.df
    if (
        right_node is None
        or right_node.df is None
        or right_node.df.shape[0] == 0
    ):
        right_df = pd.DataFrame(
            columns=[f.name for f in right_node.fields]  # type: ignore
            + [FENNEL_DELETE_TIMESTAMP]
        )
        right_df = cast_df_to_arrow_dtype(right_df, right_node.fields)  # type: ignore
    else:
        right_df = copy.deepcopy(right_node.df)
    right_timestamp_col = right_node.timestamp_field
    right_keys = right_node.key_fields
    left_keys = left_node.key_fields
    left_timestamp_col = left_node.timestamp_field
    left_join_cols = left_on or on
    # Create a dictionary of left dataframe grouped by keys
    _validate_join_fields(
        left_df,
        right_df,
        right_node,
        right_node.key_fields,
        on,
        left_on,
        right_on,
        right_fields,
    )
    left_df_dict = {}  # type: ignore
    for _, row in left_df.iterrows():
        left_group = (
            tuple(row[col] for col in left_keys),
            tuple(row[col] for col in left_join_cols),  # type: ignore
        )
        if left_group not in left_df_dict:
            left_df_dict[left_group] = []
        row_dict = row.to_dict()
        left_df_dict[left_group].append(row_dict)

    # Create a dictionary of right dataframe grouped by keys
    right_df_dict = {}  # type: ignore
    for _, row in right_df.iterrows():
        right_group = tuple(row[col] for col in right_keys)
        if right_group not in right_df_dict:
            right_df_dict[right_group] = []
        row_dict = row.to_dict()
        right_df_dict[right_group].append(row_dict)

    for _, right_rows in right_df_dict.items():
        right_rows.sort(key=lambda x: x[right_timestamp_col])

    result_rows = []
    for (left_keys, left_join_cols), left_rows in left_df_dict.items():
        right_rows = right_df_dict.get(left_join_cols, [])
        right_rows_ts = [x[right_timestamp_col] for x in right_rows]
        for left_row in left_rows:
            current_result = []
            # Find the rows that have insert timestamp in range [insert_timestamp, delete_timestamp] (inclusive)
            right_range_left = bisect.bisect_left(
                right_rows_ts, left_row[left_timestamp_col]
            )
            if (
                left_row[FENNEL_DELETE_TIMESTAMP] is pd.NaT
                or left_row[FENNEL_DELETE_TIMESTAMP] is None
            ):
                right_range_right = len(right_rows_ts)
            else:
                right_range_right = bisect.bisect_right(
                    right_rows_ts, left_row[FENNEL_DELETE_TIMESTAMP]
                )
            # The left row can also be matched with rows that have the largest timestamp before insert timestamp
            # e.g left row ts = 9 and deleted = 15, with rhs rows ts = [5, 7, 10, 12, 15], the matched rows are [7, 10, 12, 15]
            right_range_left = max(right_range_left - 1, 0)
            for right_row in right_rows[right_range_left:right_range_right]:
                result_row = _combine_rows(
                    left_row, right_row, left_node, right_node, right_fields
                )
                # If the deleted timestamp is the same as the insert timestamp, the row should be excluded
                if (
                    result_row[left_timestamp_col]
                    != result_row[FENNEL_DELETE_TIMESTAMP]
                ):
                    current_result.append(result_row)
            # Sort the current result by the left node timestamp field
            current_result.sort(key=lambda x: x[left_node.timestamp_field])
            # We need to resolve left join null scenario
            # Since LHS always exists from [insert_timestamp, delete_timestamp]
            # If there is no RHS row that matches in a timestamp range, we can just create a null row
            last_deleted = left_row[left_node.timestamp_field]
            for i in range(len(current_result)):
                if current_result[i][left_node.timestamp_field] == last_deleted:
                    result_rows.append(current_result[i])
                else:
                    deleted_timestamp = current_result[i][
                        left_node.timestamp_field
                    ]
                    null_row = _create_null_row(
                        left_row,
                        left_node,
                        right_node,
                        last_deleted,
                        deleted_timestamp,
                        right_fields,
                    )
                    result_rows.append(null_row)
                    result_rows.append(current_result[i])
                # Update the last deleted timestamp
                last_deleted = current_result[i][FENNEL_DELETE_TIMESTAMP]

            # If the last deleted timestamp is not the same as the left row delete timestamp, we need to create a null row
            if (
                last_deleted is not pd.NaT
                and last_deleted != left_row[FENNEL_DELETE_TIMESTAMP]
            ):
                null_row = _create_null_row(
                    left_row,
                    left_node,
                    right_node,
                    last_deleted,
                    left_row[FENNEL_DELETE_TIMESTAMP],
                    right_fields,
                )
                result_rows.append(null_row)
    result_rows.sort(key=lambda x: x[left_node.timestamp_field])
    merged_df = pd.DataFrame(result_rows)
    if how == "inner":
        merged_df = merged_df[merged_df[FENNEL_LOOKUP]]
    merged_df.drop(columns=[FENNEL_LOOKUP], inplace=True)
    return merged_df
