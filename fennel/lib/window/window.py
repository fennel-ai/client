from typing import List, Union

from pydantic import BaseModel

import fennel.gen.dataset_pb2 as proto  # type: ignore
from fennel.lib.duration import (
    Duration,
    duration_to_micros,
)

ItemType = Union[str, List[str]]


# ------------------------------------------------------------------------------
# Windows
# ------------------------------------------------------------------------------


class Window(BaseModel):
    start: Duration
    end: Duration

    def __init__(self, start: Duration, end: Duration = "0s"):
        super().__init__(start=start, end=end)

    def to_proto(self) -> proto.WindowSpec:
        if self.start == "forever":
            return proto.WindowSpec(forever_window=True)
        return proto.WindowSpec(
            window=proto.Window(
                start=duration_to_micros(self.start),
                end=duration_to_micros(self.end),
            )
        )

    def signature(self) -> str:
        if self.start == "forever":
            return f"forever_{self.end}"
        return f"{self.start}_{self.end}"
