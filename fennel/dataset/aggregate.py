from pydantic import BaseModel, Extra
from typing import List, Union, Optional

import fennel.gen.dataset_pb2 as proto
from fennel.utils.duration import Duration, duration_to_timedelta, \
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


class DeltaWindow(Window):
    base_window: Window
    target_window: Window

    def to_proto(self):
        return proto.DeltaWindow(
            base_window=self.base_window.to_proto(),
            target_window=self.target_window.to_proto(),
        )


# ------------------------------------------------------------------------------
# Aggregate Types
# ------------------------------------------------------------------------------


class AggregateType(BaseModel):
    window: Window

    def to_proto(self):
        raise NotImplementedError

    def validate(self):
        pass

    class Config:
        extra = Extra.forbid


class Count(AggregateType):
    agg_func = proto.AggregateType.COUNT

    def to_proto(self):
        if self.window is None:
            raise ValueError("Window must be specified for Count")

        return proto.Aggregation(
            type=self.agg_func,
            window_spec=self.window.to_proto(),
        )

    def validate(self):
        pass


class Sum(AggregateType):
    value: str
    agg_func = proto.AggregateType.SUM

    def to_proto(self):
        return proto.Aggregation(
            type=self.agg_func,
            window_spec=self.window.to_proto(),
            value_field=self.value,
        )


class Average(AggregateType):
    value: str
    agg_func = proto.AggregateType.AVG


class Max(AggregateType):
    value: str
    agg_func = proto.AggregateType.MAX


class Min(AggregateType):
    value: str
    agg_func = proto.AggregateType.MIN


class TopK(AggregateType):
    item: ItemType
    score: str
    k: int
    agg_func = proto.AggregateType.TOPK
    update_frequency: int = 60


class CF(AggregateType):
    context: ItemType
    weight: str
    limit: int
    agg_func = proto.AggregateType.CF
    update_frequency: int = 60
