from pydantic import BaseModel, Extra
from typing import List, Union

import fennel.gen.dataset_pb2 as proto
from fennel.lib.window import Window

ItemType = Union[str, List[str]]


class AggregateType(BaseModel):
    window: Window
    # Name of the field the aggregate will  be assigned to
    name: str

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
