from abc import ABC, abstractmethod

import pandas as pd
from typing import List

from fennel.lib.aggregate import AggregateType
from fennel.lib.aggregate import Count, Sum, Average, LastK
from fennel.lib.duration import duration_to_timedelta

# Type of data, 1 indicates insert -1 indicates delete.
FENNEL_ROW_TYPE = "__fennel_row_type__"
FENNEL_FAKE_OF_FIELD = "fennel_fake_of_field"


# Interface needed for an aggregate function
# 1. Add value to state
# 2. Remove value from state
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


class AvgState(AggState):
    def __init__(self):
        self.sum = 0
        self.count = 0

    def add_val_to_state(self, val):
        self.sum += val
        self.count += 1
        return self.sum / self.count

    def del_val_from_state(self, val):
        self.sum -= val
        self.count -= 1
        if self.count == 0:
            return 0
        return self.sum / self.count

    def get_val(self):
        if self.count == 0:
            return 0
        return self.sum / self.count


class LastKState(AggState):
    def __init__(self, k, dedup):
        self.k = k
        self.dedeup = dedup
        self.vals = []

    def add_val_to_state(self, val):
        if self.dedeup:
            if val in self.vals:
                return self.vals
        self.vals.append(val)
        if not self.dedeup:
            return self.vals[-self.k:]
        else:
            to_ret = []
            for v in self.vals:
                if v not in to_ret:
                    to_ret.append(v)
        return to_ret[-self.k:]

    def del_val_from_state(self, val):
        if self.dedeup:
            if val in self.vals:
                self.vals.remove(val)
        return self.vals[-self.k:]

    def get_val(self):
        return self.vals[-self.k:]


def get_aggregated_df(df: pd.DataFrame, aggregate: AggregateType,
                      ts_field: str,
                      key_fields: List[str]) -> pd.DataFrame:
    df[FENNEL_ROW_TYPE] = 1

    if aggregate.window.start != "forever":
        window_secs = duration_to_timedelta(
            aggregate.window.start).total_seconds()
        expire_secs = pd.Timedelta(seconds=window_secs) + pd.Timedelta(
            seconds=1)
        # For every row find the time it will expire and add it to the dataframe
        # as a delete row
        del_df = df.copy()
        del_df[ts_field] = del_df[ts_field] + expire_secs
        del_df[FENNEL_ROW_TYPE] = -1
        df = pd.concat([df, del_df], ignore_index=True)
        df = df.sort_values([ts_field, FENNEL_ROW_TYPE],
            ascending=[True, False])
        # Reset the index
    df = df.reset_index(drop=True)

    ret_df = pd.DataFrame()

    if isinstance(aggregate, Count):
        df[FENNEL_FAKE_OF_FIELD] = 1
        of_field = FENNEL_FAKE_OF_FIELD
    else:
        of_field = aggregate.of

    state = {}

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
            new_val = state[key].del_val_from_state(val)
            new_row = row.copy()
            new_row[aggregate.into_field] = new_val
            if aggregate.into_field != of_field:
                new_row.drop(of_field, inplace=True)
        else:
            # Add val to state
            if key not in state:
                if isinstance(aggregate, Sum):
                    state[key] = SumState()
                elif isinstance(aggregate, Count):
                    state[key] = CountState()
                elif isinstance(aggregate, Average):
                    state[key] = AvgState()
                elif isinstance(aggregate, LastK):
                    state[key] = LastKState(aggregate.limit, aggregate.dedup)
                else:
                    raise Exception("Unsupported aggregate function")
            new_val = state[key].add_val_to_state(val)
            # Create a new row with the aggregate value, row_key_fields and ts_field
            new_row = row.copy()
            new_row[aggregate.into_field] = new_val
            if aggregate.into_field != of_field:
                new_row.drop(of_field, inplace=True)
        ret_df = ret_df.append(new_row, ignore_index=True)
    # Drop the fennel_row_type column
    ret_df.drop(FENNEL_ROW_TYPE, inplace=True, axis=1)
    ret_df.fillna(0, inplace=True)
    return ret_df
