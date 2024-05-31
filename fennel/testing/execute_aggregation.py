import heapq
import math
from abc import ABC, abstractmethod
from collections import Counter
from datetime import datetime, timezone
from math import sqrt
from typing import Dict, List, Type, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from sortedcontainers import SortedList

from fennel.datasets import (
    AggregateType,
    Distinct,
    Count,
    Sum,
    Average,
    LastK,
    Min,
    Max,
    Stddev,
    Quantile,
)
from fennel.dtypes import Continuous, Hopping, Tumbling, Session
from fennel.internal_lib.duration import duration_to_timedelta
from fennel.internal_lib.schema import get_datatype
from fennel.testing.test_utils import (
    cast_col_to_arrow_dtype,
    FENNEL_DELETE_TIMESTAMP,
    add_deletes,
)

# Type of data, 1 indicates insert -1 indicates delete.
FENNEL_ROW_TYPE = "__fennel_row_type__"
FENNEL_FAKE_OF_FIELD = "fennel_fake_of_field"


class AggState(ABC):
    @abstractmethod
    def add_val_to_state(self, val):
        pass

    @abstractmethod
    def del_val_from_state(self, val):
        pass

    @abstractmethod
    def get_val(self):
        pass


class SumState(AggState):
    def __init__(self):
        self.sum = 0

    def add_val_to_state(self, val):
        self.sum += val
        return self.sum

    def del_val_from_state(self, val):
        self.sum -= val
        return self.sum

    def get_val(self):
        return self.sum


class CountState(AggState):
    def __init__(self):
        self.count = 0

    def add_val_to_state(self, val):
        self.count += 1
        return self.count

    def del_val_from_state(self, val):
        self.count -= 1
        return self.count

    def get_val(self):
        return self.count


class CountUniqueState(AggState):
    def __init__(self):
        self.count = 0
        self.counter = Counter()

    def add_val_to_state(self, val):
        self.counter[val] += 1
        self.count = len(self.counter)
        return self.count

    def del_val_from_state(self, val):
        self.counter[val] -= 1
        if self.counter[val] == 0:
            del self.counter[val]
        self.count = len(self.counter)
        return self.count

    def get_val(self):
        return self.count


class AvgState(AggState):
    def __init__(self, default):
        self.sum = 0
        self.count = 0
        self.default = default

    def add_val_to_state(self, val):
        self.sum += val
        self.count += 1
        return self.get_val()

    def del_val_from_state(self, val):
        self.sum -= val
        self.count -= 1
        return self.get_val()

    def get_val(self):
        if self.count == 0:
            return self.default
        return self.sum / self.count


class LastKState(AggState):
    def __init__(self, k, dedup):
        self.k = k
        self.dedeup = dedup
        self.vals = []

    def add_val_to_state(self, val):
        self.vals.append(val)
        if not self.dedeup:
            return list(reversed(self.vals[-self.k :]))
        else:
            to_ret = []
            for v in reversed(self.vals):
                if v not in to_ret:
                    to_ret.append(v)
                if len(to_ret) == self.k:
                    break
        return list(to_ret[: self.k])

    def del_val_from_state(self, val):
        if val in self.vals:
            self.vals.remove(val)
        if not self.dedeup:
            return list(reversed(self.vals[-self.k :]))

        ret = []
        for v in reversed(self.vals):
            if v not in ret:
                ret.append(v)
            if len(ret) == self.k:
                break
        return list(ret[: self.k])

    def get_val(self):
        return list(reversed(self.vals[-self.k :]))


class Heap:
    def __init__(self, heap_type="min"):
        self.elements = []
        self.heap_type = 1 if heap_type == "min" else -1
        self.del_elements = set()

    def __len__(self):
        return len(self.elements)

    def push(self, element):
        element = self.heap_type * element
        if element in self.del_elements:
            self.del_elements.remove(element)
        else:
            heapq.heappush(self.elements, element)

    def remove(self, element):
        element = self.heap_type * element
        if element in self.del_elements:
            return
        self.del_elements.add(element)

    def top(self):
        while len(self.elements) > 0 and self.elements[0] in self.del_elements:
            self.del_elements.remove(self.elements[0])
            heapq.heappop(self.elements)
        if len(self.elements) == 0:
            return None
        return self.elements[0] * self.heap_type


class MinState(AggState):
    def __init__(self, default: float):
        self.counter = Counter()  # type: ignore
        self.min_heap = Heap(heap_type="min")
        self.default = default

    def add_val_to_state(self, val):
        if val not in self.counter:
            self.min_heap.push(val)
        self.counter[val] += 1
        return self.min_heap.top()

    def del_val_from_state(self, val):
        if val not in self.counter:
            return self.get_val()
        if self.counter[val] == 1:
            self.min_heap.remove(val)
            self.counter.pop(val)
        else:
            self.counter[val] -= 1
        return self.get_val()

    def get_val(self):
        if len(self.min_heap) == 0:
            return self.default
        val = self.min_heap.top()
        return val if val else self.default


class MaxState(AggState):
    def __init__(self, default: float):
        self.counter = Counter()  # type: ignore
        self.max_heap = Heap(heap_type="max")
        self.default = default

    def add_val_to_state(self, val):
        if val not in self.counter:
            self.max_heap.push(val)
        self.counter[val] += 1
        return self.max_heap.top()

    def del_val_from_state(self, val):
        if val not in self.counter:
            return self.get_val()
        if self.counter[val] == 1:
            self.max_heap.remove(val)
            self.counter.pop(val)
        else:
            self.counter[val] -= 1
        return self.get_val()

    def get_val(self):
        if len(self.max_heap) == 0:
            return self.default
        val = self.max_heap.top()
        return val if val else self.default


class StddevState(AggState):
    def __init__(self, default):
        self.count = 0
        self.mean = 0
        self.m2 = 0
        self.default = default

    def add_val_to_state(self, val):
        self.count += 1
        delta = val - self.mean
        self.mean += delta / self.count
        delta2 = val - self.mean
        self.m2 += delta * delta2
        return self.get_val()

    def del_val_from_state(self, val):
        self.count -= 1
        if self.count == 0:
            self.mean = 0
            self.m2 = 0
            return self.default
        delta = val - self.mean
        self.mean -= delta / self.count
        delta2 = val - self.mean
        self.m2 -= delta * delta2
        return self.get_val()

    def get_val(self):
        if self.count == 0:
            return self.default
        variance = self.m2 / self.count
        # due to floating point imprecision, a zero variance may be represented as
        # a small negative number. In this case, stddev = sqrt(0)
        if variance < 0:
            return 0 if variance > -1e-10 else -1.0
        return sqrt(variance)


class DistinctState(AggState):
    def __init__(self):
        self.counter = Counter()

    def add_val_to_state(self, val):
        self.counter[val] += 1
        return list(self.counter.keys())

    def del_val_from_state(self, val):
        self.counter[val] -= 1
        if self.counter[val] == 0:
            del self.counter[val]
        return list(self.counter.keys())

    def get_val(self) -> List:
        return list(self.counter.keys())


class QuantileState(AggState):
    def __init__(self, default, p):
        self.p = p
        self.default = default
        self.vals = SortedList()

    def add_val_to_state(self, val):
        self.vals.add(val)
        return self.get_val()

    def del_val_from_state(self, val):
        self.vals.remove(val)
        return self.get_val()

    def get_val(self) -> List:
        if len(self.vals) == 0:
            return self.default
        index = int(math.floor(self.p * len(self.vals))) + 1
        return self.vals[min(len(self.vals) - 1, max(index - 1, 0))]


def get_timestamps_for_hopping_window(
    timestamp: datetime, duration: int, stride: int
) -> List[datetime]:
    """
    Given a window duration, stride and a timestamp first fetch all the windows in which the
    given timestamp lies then return the list of window end timestamps.
    """
    current_ts = int(timestamp.timestamp())

    # Get the window of which current timestamp is part of
    start_ts = (((current_ts - duration) // stride) + 1) * stride

    results = []

    # Getting all the window of which timestamp is a part of
    while start_ts <= current_ts:
        results.append(
            datetime.fromtimestamp(start_ts + duration, tz=timezone.utc)
        )
        start_ts = start_ts + stride

    return results


def get_timestamps_for_session_window(
    df: pd.DataFrame, key_fields: List[str], ts_field: str, gap: int
) -> pd.DataFrame:
    """
    Given a session gap first the session in which the given timestamp lies then return end timestamp of the session.
    """
    df = df.sort_values(by=ts_field).reset_index(drop=True)
    sessions: List[dict] = []
    curr_session = None
    for _, row in df.iterrows():
        timestamp = int(row[ts_field].timestamp())
        if curr_session is None:
            curr_session = {"ts": timestamp, "rows": [row]}
        else:
            if timestamp <= curr_session["ts"] + gap:
                curr_session["ts"] = timestamp
                curr_session["rows"].append(row)
            else:
                sessions.append(curr_session)
                curr_session = {"ts": timestamp, "rows": [row]}
    if curr_session is not None:
        sessions.append(curr_session)
    rows = []
    for session in sessions:
        timestamp = datetime.fromtimestamp(session["ts"], tz=timezone.utc)  # type: ignore
        for row in session["rows"]:
            row[ts_field] = timestamp
            rows.append(row)

    # Dropping key columns
    return pd.DataFrame(rows).drop(columns=key_fields)


def preprocess_df_continuous_window(
    input_df: pd.DataFrame,
    window: Continuous,
    ts_field: str,
) -> pd.DataFrame:
    if window.duration != "forever":
        window_secs = duration_to_timedelta(window.duration).total_seconds()
        expire_secs = np.timedelta64(int(window_secs), "s") + np.timedelta64(
            1, "s"
        )

        # For every row find the time it will expire and add it to the dataframe
        # as a delete row
        del_df = input_df.copy()
        del_df[ts_field] = del_df[ts_field] + expire_secs
        del_df[FENNEL_ROW_TYPE] = -1

        # If the input dataframe has a delete timestamp field, pick the minimum
        # of the two timestamps
        if FENNEL_DELETE_TIMESTAMP in input_df.columns:
            del_df[ts_field] = del_df[[ts_field, FENNEL_DELETE_TIMESTAMP]].min(
                axis=1
            )
        input_df = pd.concat([input_df, del_df], ignore_index=True)
    elif FENNEL_DELETE_TIMESTAMP in input_df.columns:
        del_df = input_df.copy()
        del_df = del_df[del_df[FENNEL_DELETE_TIMESTAMP].notna()]
        del_df[FENNEL_ROW_TYPE] = -1
        del_df[ts_field] = del_df[FENNEL_DELETE_TIMESTAMP]
        input_df = pd.concat([input_df, del_df], ignore_index=True)
    return input_df


def preprocess_df_session_window(
    input_df: pd.DataFrame,
    window: Session,
    key_fields: List[str],
    ts_field: str,
) -> pd.DataFrame:
    gap = window.gap_total_seconds()
    cols = input_df.columns
    input_df = (
        (
            input_df.groupby(key_fields)
            .apply(
                get_timestamps_for_session_window,
                key_fields=key_fields,
                ts_field=ts_field,
                gap=gap,
            )
            .reset_index()
        )
        .loc[:, cols]
        .assign(
            **{
                ts_field: lambda x: x[ts_field].astype(
                    pd.ArrowDtype(pa.timestamp("ns", "UTC"))
                )
            }
        )
    )

    fennel_internal_field = "__fennel_internal_field__"

    # Getting all the unique sessions against all the keys
    internal_df = (
        input_df.copy()[key_fields + [ts_field]]
        .drop_duplicates()
        .sort_values(by=ts_field)
        .reset_index(drop=True)
    )
    # This groups the dataset by key_fields then fetches the next value,
    # this ends up returning the end timestamp of next session
    internal_df[fennel_internal_field] = internal_df.groupby(key_fields)[
        ts_field
    ].shift(-1)
    internal_df = internal_df[internal_df[fennel_internal_field].notna()]

    # Join with input_df.copy() to get the end timestamp against each row.
    del_df = input_df.copy().merge(
        internal_df, how="inner", on=key_fields + [ts_field]
    )
    del_df[ts_field] = del_df[fennel_internal_field]
    del_df.drop(columns=[fennel_internal_field], inplace=True)
    del_df[FENNEL_ROW_TYPE] = -1

    # If the input dataframe has a delete timestamp field, pick the minimum
    # of the two timestamps
    if FENNEL_DELETE_TIMESTAMP in input_df.columns:
        del_df[ts_field] = del_df[[ts_field, FENNEL_DELETE_TIMESTAMP]].min(
            axis=1
        )
    input_df = pd.concat([input_df, del_df], ignore_index=True)
    return input_df


def preprocess_df_discrete_window(
    input_df: pd.DataFrame,
    window: Union[Hopping, Tumbling],
    ts_field: str,
) -> pd.DataFrame:
    """
    According to Aggregation Specification preprocess the dataframe to add delete or inserts.
    """
    if window.duration != "forever":
        duration = window.duration_total_seconds()
    else:
        duration = window.stride_total_seconds()
    stride = window.stride_total_seconds()

    # Add the inserts for which row against end timestamp of every window in which the row belongs to
    input_df = (
        input_df.assign(
            **{
                ts_field: lambda x: x[ts_field].apply(
                    lambda y: get_timestamps_for_hopping_window(
                        y, duration, stride
                    )
                )
            }
        )
        .explode(ts_field)
        .assign(
            **{
                ts_field: lambda x: x[ts_field].astype(
                    pd.ArrowDtype(pa.timestamp("ns", "UTC"))
                )
            }
        )
        .reset_index(drop=True)
    )

    if window.duration != "forever":
        # Add deletes for each row as end of next window.
        del_df = input_df.copy()
        expire_secs = np.timedelta64(stride, "s")
        del_df[ts_field] = del_df[ts_field] + expire_secs
        del_df[FENNEL_ROW_TYPE] = -1
        # If the input dataframe has a delete timestamp field, pick the minimum
        # of the two timestamps
        if FENNEL_DELETE_TIMESTAMP in input_df.columns:
            del_df[ts_field] = del_df[[ts_field, FENNEL_DELETE_TIMESTAMP]].min(
                axis=1
            )
        input_df = pd.concat([input_df, del_df], ignore_index=True)
    else:
        # forever hopping aggregates on keyed datasets
        if FENNEL_DELETE_TIMESTAMP in input_df.columns:
            del_df = input_df.copy()
            del_df = del_df[del_df[FENNEL_DELETE_TIMESTAMP].notna()]
            del_df[FENNEL_ROW_TYPE] = -1
            del_df[ts_field] = del_df[FENNEL_DELETE_TIMESTAMP]
            input_df = pd.concat([input_df, del_df], ignore_index=True)
    return input_df


def preprocess_df(
    input_df: pd.DataFrame,
    aggregate: AggregateType,
    key_fields: List[str],
    ts_field: str,
) -> pd.DataFrame:
    """
    Preprocess the dataframe to add delete or inserts according to window type.
    """
    if isinstance(aggregate.window, Continuous):
        input_df = preprocess_df_continuous_window(
            input_df, aggregate.window, ts_field
        )
    elif isinstance(aggregate.window, (Hopping, Tumbling)):
        input_df = preprocess_df_discrete_window(
            input_df, aggregate.window, ts_field
        )
    else:
        input_df = preprocess_df_session_window(
            input_df, aggregate.window, key_fields, ts_field
        )
    return input_df.sort_values(
        [ts_field, FENNEL_ROW_TYPE], ascending=[True, False]
    ).reset_index(drop=True)


def get_aggregated_df(
    input_df: pd.DataFrame,
    aggregate: AggregateType,
    ts_field: str,
    key_fields: List[str],
    output_dtype: Type,
) -> pd.DataFrame:
    df = input_df.copy()
    df[FENNEL_ROW_TYPE] = 1

    df = preprocess_df(df, aggregate, key_fields, ts_field)

    # Reset the index
    df = df.reset_index(drop=True)
    if isinstance(aggregate, Count) and not aggregate.unique:
        df[FENNEL_FAKE_OF_FIELD] = 1
        of_field = FENNEL_FAKE_OF_FIELD
    else:
        if not hasattr(aggregate, "of"):
            raise Exception("Aggregate must have an of field")
        of_field = aggregate.of  # type: ignore

    state: Dict[int, AggState] = {}
    result_vals = []
    for i, row in df.iterrows():
        val = row.loc[of_field]
        row_key_fields = []
        for key_field in key_fields:
            row_key_fields.append(row.loc[key_field])
        key = hash(tuple(row_key_fields))
        # If the row is a delete row, delete from state.
        if row[FENNEL_ROW_TYPE] == -1:
            if key not in state:
                raise Exception("Delete row without insert row")
            result_vals.append(state[key].del_val_from_state(val))
        else:
            # Add val to state
            if key not in state:
                if isinstance(aggregate, Sum):
                    state[key] = SumState()
                elif isinstance(aggregate, Count):
                    if aggregate.unique:
                        state[key] = CountUniqueState()
                    else:
                        state[key] = CountState()
                elif isinstance(aggregate, Average):
                    state[key] = AvgState(aggregate.default)
                elif isinstance(aggregate, LastK):
                    state[key] = LastKState(aggregate.limit, aggregate.dedup)
                elif isinstance(aggregate, Min):
                    state[key] = MinState(aggregate.default)
                elif isinstance(aggregate, Max):
                    state[key] = MaxState(aggregate.default)
                elif isinstance(aggregate, Stddev):
                    state[key] = StddevState(aggregate.default)
                elif isinstance(aggregate, Distinct):
                    state[key] = DistinctState()
                elif isinstance(aggregate, Quantile):
                    state[key] = QuantileState(aggregate.default, aggregate.p)
                else:
                    raise Exception(
                        f"Unsupported aggregate function {aggregate}"
                    )
            result_vals.append(state[key].add_val_to_state(val))

    df[aggregate.into_field] = result_vals
    if aggregate.into_field != of_field:
        df.drop(of_field, inplace=True, axis=1)
    # Drop the fennel_row_type column
    df.drop(FENNEL_ROW_TYPE, inplace=True, axis=1)
    subset = key_fields + [ts_field]
    df = df.drop_duplicates(subset=subset, keep="last")
    df = df.reset_index(drop=True)

    data_type = get_datatype(output_dtype)
    df[aggregate.into_field] = cast_col_to_arrow_dtype(
        df[aggregate.into_field], data_type
    )
    df = add_deletes(df, key_fields, ts_field)
    return df
