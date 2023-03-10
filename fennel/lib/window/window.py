from typing import List, Union

from pydantic import BaseModel
from datetime import timedelta

import fennel.gen.window_pb2 as window_proto
import google.protobuf.duration_pb2 as duration_proto
from fennel.lib.duration import Duration, duration_to_timedelta

ItemType = Union[str, List[str]]


# ------------------------------------------------------------------------------
# Windows
# ------------------------------------------------------------------------------


class Window(BaseModel):
    start: Duration
    end: Duration

    def __init__(self, start: Duration, end: Duration = "0s"):
        super().__init__(start=start, end=end)

    def is_forever(self):
        return self.start == "forever"

    # TODO(mohit, aditya): Consider using `end` as well once there is support for
    # different types of windows
    def sliding_window_duration(self) -> timedelta:
        return duration_to_timedelta(self.start)

    def to_proto(self) -> window_proto:
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
