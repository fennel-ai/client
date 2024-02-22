from fennel.internal_lib.duration.duration import duration_to_timedelta


def test_duration():
    assert duration_to_timedelta("28d") == duration_to_timedelta("4w")
    assert duration_to_timedelta("365d") == duration_to_timedelta("1y")
    assert duration_to_timedelta("1m") == duration_to_timedelta("60s")
    assert duration_to_timedelta("1w") == duration_to_timedelta("7d")
    assert duration_to_timedelta("1w") == duration_to_timedelta("10080m")
    assert duration_to_timedelta("1w 1d") == duration_to_timedelta("8d")
    assert duration_to_timedelta("3d 2h 1m") == duration_to_timedelta("4441m")
    assert duration_to_timedelta("3d 2h  1m") == duration_to_timedelta("4441m")
