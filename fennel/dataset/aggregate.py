from dataclasses import dataclass
from pydantic import BaseModel
from typing import List, Union

import fennel.gen.dataset_pb2 as proto
from fennel.utils.duration import Duration, duration_to_timedelta, \
    timedelta_to_micros

ItemType = Union[str, List[str]]


class AggregateType(BaseModel):
    window: Duration

    def to_proto(self):
        raise NotImplementedError

    def validate(self):
        pass


class Count(AggregateType):
    agg_func = proto.AggregateType.COUNT

    def to_proto(self):
        return proto.Aggregation(
            type=self.agg_func,
            window_spec=_window_to_proto(self.window)
        )

    def validate(self):
        print("Count.validate")
        print(self.__dict__)
        pass


class Sum(AggregateType):
    value: str
    agg_func = proto.AggregateType.SUM

    def to_proto(self):
        return proto.Aggregation(
            type=self.agg_func,
            window_spec=_window_to_proto(self.window),
            value_field=self.value,
        )


class Average(AggregateType):
    value: str
    agg_func = proto.AggregateType.AVG


class Max(AggregateType):
    value: str
    agg_func = proto.AggregateType.MAX


@dataclass(frozen=True)
class Min(AggregateType):
    value: str
    agg_func = proto.AggregateType.MIN


@dataclass(frozen=True)
class TopK(AggregateType):
    item: ItemType
    score: str
    k: int
    agg_func = proto.AggregateType.TOPK
    update_frequency: int = 60


@dataclass(frozen=True)
class CF(AggregateType):
    context: ItemType
    weight: str
    limit: int
    agg_func = proto.AggregateType.CF
    update_frequency: int = 60


# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------


def _window_to_proto(window) -> proto.WindowSpec:
    if window == "forever":
        return proto.WindowSpec(
            forever_window=True,
        )
    elif ":" in window:
        # TODO: Decide on delta window syntax
        raise NotImplementedError
    elif "->" in window:
        start, end = window.split("->")
        start_micros = timedelta_to_micros(duration_to_timedelta(start))
        end_micros = timedelta_to_micros(duration_to_timedelta(end))
        return proto.WindowSpec(
            window=proto.Window(
                start=start_micros,
                end=end_micros,
            )
        )
    else:
        window_micros = timedelta_to_micros(duration_to_timedelta(window))
        return proto.WindowSpec(window=proto.Window(start=window_micros, end=0))
