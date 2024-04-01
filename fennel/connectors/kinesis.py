from datetime import datetime

# Union is needed pre Python 3.10
from typing import Union


# This class represents the AT_TIMESTAMP init position for a Kinesis stream.
# Valid timestamps values are:
# - datetime
# - int (unix timestamp) in seconds
# - float (unix timestamp) in seconds with fractional part
# - str: ISO 8601 format, such as
#   YYYY-MM-DD
#   YYYY-MM-DDTHH:MM:SS
#   YYYY-MM-DD HH:MM:SSZ
#   YYYY-MM-DD HH:MM:SS.ffffff
#   YYYY-MM-DD HH:MM:SS.ffffff-0800
class at_timestamp(object):
    timestamp: datetime

    def __init__(self, timestamp: Union[datetime, int, float, str]):
        if isinstance(timestamp, datetime):
            self.timestamp = timestamp
        elif isinstance(timestamp, (int, float)):
            self.timestamp = datetime.utcfromtimestamp(timestamp)
        elif isinstance(timestamp, str):
            self.timestamp = datetime.fromisoformat(timestamp)
        else:
            raise TypeError(
                f"Kinesis init_position must be 'latest', 'trim_horizon' or a timestamp. Invalid timestamp type {type(timestamp)}"
            )

    def __call__(self) -> datetime:
        return self.timestamp
