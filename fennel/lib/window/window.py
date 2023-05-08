from datetime import timedelta
from typing import List, Union

import google.protobuf.duration_pb2 as duration_proto  # type: ignore
from fennel._vendor.pydantic import BaseModel  # type: ignore

import fennel.gen.window_pb2 as window_proto
from fennel.lib.duration import (
    Duration,
    duration_to_timedelta,
    is_valid_duration,
)

ItemType = Union[str, List[str]]


# ------------------------------------------------------------------------------
# Windows
# ------------------------------------------------------------------------------


class Window(BaseModel):
    start: Duration
    end: Duration

    def __init__(self, start: Duration, end: Duration = "0s"):
        if start == "forever" and end != "0s":
            raise ValueError("Cannot specify an end for a forever window")
        if start != "forever":
            # Check that start is a valid duration
            if is_valid_duration(start) is not None:
                raise is_valid_duration(start)  # type: ignore
        super().__init__(start=start, end=end)

    def is_forever(self):
        return self.start == "forever"

    # TODO(mohit, aditya): Consider using `end` as well once there is support for
    # different types of windows
    def sliding_window_duration(self) -> timedelta:
        return duration_to_timedelta(self.start)

    def to_proto(self) -> window_proto.Window:
        if self.is_forever():
            return window_proto.Window(forever=window_proto.Forever())

        duration = duration_proto.Duration()
        duration.FromTimedelta(self.sliding_window_duration())
        return window_proto.Window(
            sliding=window_proto.Sliding(duration=duration)
        )

    def signature(self) -> str:
        if self.start == "forever":
            return f"forever_{self.end}"
        return f"{self.start}_{self.end}"
