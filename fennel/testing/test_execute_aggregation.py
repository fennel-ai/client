from datetime import datetime, timezone
from math import sqrt

import pandas as pd

from fennel.testing.execute_aggregation import (
    SumState,
    AvgState,
    CountState,
    CountUniqueState,
    LastKState,
    MinState,
    MaxState,
    StddevState,
    DistinctState,
    get_timestamps_for_hopping_window,
    get_timestamps_for_session_window,
)


def test_sum_state():
    state = SumState()
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 3
    assert state.add_val_to_state(3) == 6
    assert state.del_val_from_state(2) == 4
    assert state.del_val_from_state(1) == 3
    assert state.add_val_to_state(3) == 6
    assert state.get_val() == 6


def test_count_state():
    state = CountState()
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 2
    assert state.add_val_to_state(3) == 3
    assert state.del_val_from_state(2) == 2
    assert state.del_val_from_state(1) == 1
    assert state.add_val_to_state(3) == 2
    assert state.get_val() == 2


def test_count_unique_state():
    state = CountUniqueState()
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 2
    assert state.add_val_to_state(3) == 3
    assert state.del_val_from_state(2) == 2
    assert state.del_val_from_state(1) == 2
    assert state.del_val_from_state(1) == 1
    assert state.add_val_to_state(3) == 1


def test_avg_state():
    state = AvgState(-1.0)
    assert state.get_val() == -1.0
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 1.5
    assert state.add_val_to_state(3) == 2.0
    assert state.del_val_from_state(2) == 2
    assert state.del_val_from_state(1) == 3
    assert state.get_val() == 3
    assert state.del_val_from_state(3) == -1.0
    assert state.get_val() == -1.0


def test_lastk_state():
    state = LastKState(k=3, dedup=False)
    assert state.add_val_to_state(1) == [1]
    assert state.add_val_to_state(2) == [2, 1]
    assert state.add_val_to_state(3) == [3, 2, 1]
    assert state.add_val_to_state(4) == [4, 3, 2]
    assert state.add_val_to_state(5) == [5, 4, 3]

    assert state.del_val_from_state(3) == [5, 4, 2]
    assert state.del_val_from_state(4) == [5, 2, 1]
    assert state.del_val_from_state(5) == [2, 1]
    assert state.del_val_from_state(5) == [2, 1]


def test_lastk_state_dedup():
    state = LastKState(k=3, dedup=True)
    assert state.add_val_to_state(1) == [1]
    assert state.add_val_to_state(2) == [2, 1]
    assert state.add_val_to_state(1) == [1, 2]
    assert state.add_val_to_state(3) == [3, 1, 2]
    assert state.add_val_to_state(4) == [4, 3, 1]
    assert state.add_val_to_state(1) == [1, 4, 3]

    assert state.del_val_from_state(3) == [1, 4, 2]
    assert state.del_val_from_state(4) == [1, 2]


def test_min_state():
    state = MinState(default=3.0)
    assert state.get_val() == 3.0
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 1
    assert state.add_val_to_state(3) == 1
    assert state.del_val_from_state(2) == 1
    assert state.del_val_from_state(1) == 3
    assert state.add_val_to_state(3) == 3
    assert state.get_val() == 3
    assert state.del_val_from_state(3) == 3
    assert state.get_val() == 3
    assert state.del_val_from_state(3) == 3.0


def test_max_state():
    state = MaxState(default=3.0)
    assert state.get_val() == 3.0
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 2
    assert state.add_val_to_state(3) == 3
    assert state.del_val_from_state(2) == 3
    assert state.del_val_from_state(1) == 3
    assert state.add_val_to_state(3) == 3
    assert state.get_val() == 3
    assert state.del_val_from_state(3) == 3
    assert state.get_val() == 3
    assert state.del_val_from_state(3) == 3.0


def test_stddev_state():
    state = StddevState(default=-1.0)
    assert state.add_val_to_state(1) == 0
    assert state.add_val_to_state(1) == 0
    assert state.add_val_to_state(10) == sqrt(18)
    assert state.del_val_from_state(1) == 4.5
    assert state.del_val_from_state(1) == 0
    assert state.get_val() == 0
    assert state.del_val_from_state(10) == -1.0


def test_distinct_state():
    state = DistinctState()
    assert state.add_val_to_state(1) == [1]
    assert state.add_val_to_state(1) == [1]
    assert state.add_val_to_state(2) == [1, 2]
    assert state.get_val() == [1, 2]
    assert state.add_val_to_state(3) == [1, 2, 3]
    assert state.del_val_from_state(2) == [1, 3]
    assert state.del_val_from_state(1) == [1, 3]
    assert state.add_val_to_state(3) == [1, 3]
    assert state.get_val() == [1, 3]
    assert state.del_val_from_state(3) == [1, 3]


def test_get_timestamps_for_hopping_window():
    timestamp = datetime(2020, 1, 2, 13, 0, 0, tzinfo=timezone.utc)

    secs_1d = 24 * 60 * 60
    secs_1h = 60 * 60

    # Test tumbling window of duration 1d.
    assert [
        datetime(2020, 1, 3, 0, 0, 0, tzinfo=timezone.utc)
    ] == get_timestamps_for_hopping_window(timestamp, secs_1d, secs_1d)

    # Test Hopping window of duration 1d stride 1h.
    assert get_timestamps_for_hopping_window(timestamp, secs_1d, secs_1h) == [
        datetime(2020, 1, 2, 14, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 15, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 16, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 17, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 18, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 19, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 20, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 21, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 22, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 23, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 0, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 1, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 2, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 3, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 4, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 5, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 6, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 7, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 8, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 9, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 10, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 11, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 12, 0, tzinfo=timezone.utc),
        datetime(2020, 1, 3, 13, 0, tzinfo=timezone.utc),
    ]


def test_get_timestamps_for_session_window():
    # test session end timestamps for gap of 1 secs
    data = pd.DataFrame(
        {
            "a": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            "ts_field": [
                datetime(2020, 1, 2, 13, 0, 0, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 1, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 2, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 6, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 7, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 1, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 2, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 5, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 6, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 7, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 8, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 9, tzinfo=timezone.utc),
                datetime(2020, 1, 2, 13, 0, 14, tzinfo=timezone.utc),
            ],
        }
    )

    output = (
        data.groupby("a")
        .apply(
            get_timestamps_for_session_window,
            key_fields=["a"],
            ts_field="ts_field",
            gap=1,
        )
        .reset_index()
    )

    assert output["ts_field"].to_list() == [
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 7, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 7, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 3, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 9, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 9, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 9, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 9, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 9, tzinfo=timezone.utc),
        datetime(2020, 1, 2, 13, 0, 14, tzinfo=timezone.utc),
    ]
