from fennel.utils.duration import duration_to_timedelta


def test_Duration():
    assert duration_to_timedelta("1m") == duration_to_timedelta("30d")
    assert duration_to_timedelta("12m 5d") == duration_to_timedelta("1y")
    assert duration_to_timedelta("1M") == duration_to_timedelta("60S")
    assert duration_to_timedelta("1w") == duration_to_timedelta("7d")
    assert duration_to_timedelta("1m") == duration_to_timedelta("43200M")
