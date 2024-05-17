from datetime import datetime, timezone
from typing import Tuple, Union

import pandas as pd

from fennel.dtypes import Window, Hopping, Tumbling
from fennel.internal_lib.duration import Duration, duration_to_timedelta


def bucketize(
    timestamp: Union[datetime, pd.Timestamp],
    window: Union[Duration, Tuple[Duration, Duration], Hopping, Tumbling],
) -> pd.Series:
    """
    This helper function applies a lambda function on the timestamp column mentioned in the parameter.
    Lambda function fetches the last window which ended just before the given timestamp.

    Parameters:
    ----------
    :param timestamp: Timestamp value
    :param window: Can be four types:
        1. If string is passed like "1hr" then considers a tumbling window of duration 1hr.
        2. If tuple of strings is passed like ("1d", "1hr") then considers a sliding window of duration 1d with stride 1hr.
        3. If Hopping is passed like Sliding("1d", "1h") then considers a sliding window of duration 1d with stride 1hr.
        4. If Tumbling is passed like Tumbling("1d") then considers a tumbling window of duration 1day.

    Returns: pd.Series:
    ----------
        Returns pd.Series containing window for each timestamp present in the ts_field column of the input dataframe.

    """
    if isinstance(window, Duration):
        duration = int(duration_to_timedelta(window).total_seconds())
        stride = int(duration_to_timedelta(window).total_seconds())
    elif isinstance(window, tuple):
        duration = int(duration_to_timedelta(window[0]).total_seconds())
        stride = int(duration_to_timedelta(window[1]).total_seconds())
    elif isinstance(window, Tumbling):
        duration = int(duration_to_timedelta(window.duration).total_seconds())
        stride = int(duration_to_timedelta(window.duration).total_seconds())
    elif isinstance(window, Hopping):
        duration = int(duration_to_timedelta(window.duration).total_seconds())
        stride = int(duration_to_timedelta(window.stride).total_seconds())
    else:
        raise ValueError("Unsupported window type")

    current_ts = int(timestamp.timestamp())

    # Get the window of which current timestamp is part of
    start_ts = (((current_ts - duration) // stride) + 1) * stride
    end_ts = start_ts + duration

    # Getting the window which just ended before the timestamp
    while end_ts > current_ts:
        end_ts = end_ts - stride
        start_ts = start_ts - stride

    begin = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    return Window(begin=begin, end=end)  # type: ignore
