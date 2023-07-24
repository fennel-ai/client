from fennel.test_lib.execute_aggregation import (
    SumState,
    AvgState,
    CountState,
    CountUniqueState,
    LastKState,
    MinState,
    MaxState,
    MinForeverState,
    MaxForeverState,
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
    state = AvgState()
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 1.5
    assert state.add_val_to_state(3) == 2
    assert state.del_val_from_state(2) == 2
    assert state.del_val_from_state(1) == 3
    assert state.get_val() == 3
    assert state.del_val_from_state(3) == 0
    assert state.get_val() == 0


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
    assert state.del_val_from_state(3) is None


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
    assert state.del_val_from_state(3) is None


def test_min_forever_state():
    state = MinForeverState(default=3.0)
    assert state.get_val() == 3.0
    assert state.add_val_to_state(4) == 4
    assert state.add_val_to_state(1) == 1
    assert state.add_val_to_state(2) == 1
    assert state.add_val_to_state(3) == 1


def test_max_forever_state():
    state = MaxForeverState(default=3.0)
    assert state.get_val() == 3.0
    assert state.add_val_to_state(4) == 4
    assert state.add_val_to_state(1) == 4
    assert state.add_val_to_state(2) == 4
    assert state.add_val_to_state(3) == 4
    assert state.add_val_to_state(7) == 7
