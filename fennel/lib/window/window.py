from typing import List, Union, Optional

from pydantic import BaseModel

import fennel.gen.dataset_pb2 as proto
from fennel.lib.duration.duration import Duration, duration_to_timedelta, \
    timedelta_to_micros

ItemType = Union[str, List[str]]


# ------------------------------------------------------------------------------
# Windows
# ------------------------------------------------------------------------------


class Window(BaseModel):
    start: Optional[Duration]
    end: Duration

    def __init__(self, start: Optional[Duration] = None, end: Duration = "0s"):
        super().__init__(start=start, end=end)

    def to_proto(self):
        if self.start is None:
            return proto.WindowSpec(forever_window=True)
        return proto.WindowSpec(
            window=proto.Window(
                start=timedelta_to_micros(duration_to_timedelta(self.start)),
                end=timedelta_to_micros(duration_to_timedelta(self.end)),
            )
        )

    def signature(self):
        if self.start is None:
            return f"forever_{self.end}"
        return f"{self.start}_{self.end}"
