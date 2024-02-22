from typing import List, Union, Optional

import fennel.gen.spec_pb2 as spec_proto
from fennel._vendor.pydantic import BaseModel, Extra, validator  # type: ignore
from fennel.internal_lib.last import Last

ItemType = Union[str, List[str]]


class AggregateType(BaseModel):
    window: Last
    # Name of the field the aggregate will  be assigned to
    into_field: str

    @validator("window", pre=True)
    # Converting the window into Last object
    def validate_window(cls, value):
        if isinstance(value, str):
            return Last(value)
        else:
            return value

    def to_proto(self) -> spec_proto.PreSpec:
        raise NotImplementedError

    def validate(self):
        pass

    def signature(self):
        raise NotImplementedError

    class Config:
        extra = Extra.forbid


class Count(AggregateType):
    of: Optional[str] = None
    unique: bool = False
    approx: bool = False

    def to_proto(self):
        if self.window is None:
            raise ValueError("Window must be specified for Count")

        return spec_proto.PreSpec(
            count=spec_proto.Count(
                window=self.window.to_proto(),
                name=self.into_field,
                unique=self.unique,
                approx=self.approx,
                of=self.of,
            )
        )

    def validate(self):
        if not self.unique:
            return None

        if not self.approx:
            return NotImplementedError(
                "Exact unique counts are not yet supported, please set approx=True"
            )

        if self.of is None:
            return ValueError(
                "Count unique requires a field parameter on which unique "
                "values can be counted"
            )

    def signature(self):
        return f"count_{self.window.signature()}"


class Distinct(AggregateType):
    of: str
    unordered: bool

    def to_proto(self) -> spec_proto.PreSpec:
        return spec_proto.PreSpec(
            distinct=spec_proto.Distinct(
                window=self.window.to_proto(),
                name=self.into_field,
                of=self.of,
            )
        )

    def validate(self):
        if not self.unordered:
            return NotImplementedError("Distinct requires unordered=True")

    def signature(self):
        return f"distinct_{self.of}_{self.window.signature()}"


class Sum(AggregateType):
    of: str

    def to_proto(self):
        return spec_proto.PreSpec(
            sum=spec_proto.Sum(
                window=self.window.to_proto(),
                name=self.into_field,
                of=self.of,
            )
        )

    def signature(self):
        return f"sum_{self.of}_{self.window.signature()}"


class Average(AggregateType):
    of: str
    default: float = 0.0

    def to_proto(self):
        return spec_proto.PreSpec(
            average=spec_proto.Average(
                window=self.window.to_proto(),
                name=self.into_field,
                of=self.of,
                default=self.default,
            )
        )

    def signature(self):
        return f"avg_{self.of}_{self.window.signature()}"


class Max(AggregateType):
    of: str
    default: float

    def to_proto(self):
        return spec_proto.PreSpec(
            max=spec_proto.Max(
                window=self.window.to_proto(),
                name=self.into_field,
                of=self.of,
                default=self.default,
            )
        )

    def signature(self):
        return f"max_{self.of}_{self.window.signature()}"

    def agg_type(self):
        return "max"


class Min(AggregateType):
    of: str
    default: float

    def to_proto(self):
        return spec_proto.PreSpec(
            min=spec_proto.Min(
                window=self.window.to_proto(),
                name=self.into_field,
                of=self.of,
                default=self.default,
            )
        )

    def signature(self):
        return f"min_{self.of}_{self.window.signature()}"

    def agg_type(self):
        return "min"


class LastK(AggregateType):
    of: str
    limit: int
    dedup: bool

    def to_proto(self):
        return spec_proto.PreSpec(
            last_k=spec_proto.LastK(
                window=self.window.to_proto(),
                name=self.into_field,
                of=self.of,
                limit=self.limit,
                dedup=self.dedup,
            )
        )

    def signature(self):
        return f"lastk_{self.of}_{self.window.signature()}"


class Stddev(AggregateType):
    of: str
    default: float = -1.0

    def to_proto(self):
        return spec_proto.PreSpec(
            stddev=spec_proto.Stddev(
                window=self.window.to_proto(),
                name=self.into_field,
                default=self.default,
                of=self.of,
            )
        )

    def signature(self):
        return f"stddev_{self.of}_{self.window.signature()}"
