from typing import List, Union

from pydantic import BaseModel, Extra

import fennel.gen.dataset_pb2 as proto
from fennel.lib.window import Window

ItemType = Union[str, List[str]]


class AggregateType(BaseModel):
    window: Window
    # Name of the field the aggregate will  be assigned to
    into_field: str

    def to_proto(self):
        raise NotImplementedError

    def validate(self):
        pass

    def signature(self):
        raise NotImplementedError

    class Config:
        extra = Extra.forbid


class Count(AggregateType):
    agg_func = proto.AggregateType.COUNT

    def to_proto(self):
        if self.window is None:
            raise ValueError("Window must be specified for Count")

        return proto.Aggregation(
            agg_type=self.agg_func,
            field=self.into_field,
            window_spec=self.window.to_proto(),
        )

    def validate(self):
        pass

    def signature(self):
        return f"count_{self.window.signature()}"


class Sum(AggregateType):
    of: str
    agg_func = proto.AggregateType.SUM

    def to_proto(self):
        return proto.Aggregation(
            agg_type=self.agg_func,
            field=self.into_field,
            window_spec=self.window.to_proto(),
            value_field=self.of,
        )

    def signature(self):
        return f"sum_{self.of}_{self.window.signature()}"

    def agg_type(self):
        return "sum"


class Average(AggregateType):
    of: str
    agg_func = proto.AggregateType.AVG

    def to_proto(self):
        return proto.Aggregation(
            agg_type=self.agg_func,
            field=self.into_field,
            window_spec=self.window.to_proto(),
            value_field=self.of,
        )

    def signature(self):
        return f"avg_{self.of}_{self.window.signature()}"

    def agg_type(self):
        return "mean"


class Max(AggregateType):
    of: str
    agg_func = proto.AggregateType.MAX

    def to_proto(self):
        return proto.Aggregation(
            agg_type=self.agg_func,
            field=self.into_field,
            window_spec=self.window.to_proto(),
            value_field=self.of,
        )

    def signature(self):
        return f"max_{self.of}_{self.window.signature()}"

    def agg_type(self):
        return "max"


class Min(AggregateType):
    of: str
    agg_func = proto.AggregateType.MIN

    def to_proto(self):
        return proto.Aggregation(
            agg_type=self.agg_func,
            field=self.into_field,
            window_spec=self.window.to_proto(),
            value_field=self.of,
        )

    def signature(self):
        return f"min_{self.of}_{self.window.signature()}"

    def agg_type(self):
        return "min"


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
