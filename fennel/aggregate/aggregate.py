import dataclasses
import inspect
from typing import Any, List, Optional, Union

import cloudpickle
import pandas as pd

import fennel.gen.aggregate_pb2 as proto
from fennel.errors import NameException
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.status_pb2 import Status
from fennel.lib.schema import Schema
from fennel.lib.windows import Window


def wrap_preaggregate(cls, fn, schema):
    def wrapper(df):
        result = fn.__func__(cls, df)
        field2types = schema.get_fields_and_types()
        # Shallow Type Check
        for col, dtype in zip(result.columns, result.dtypes):
            if col not in field2types:
                raise Exception("Column {} not in schema".format(col))
            if not field2types[col].type_check(dtype):
                raise Exception(
                    f"Column {col} type mismatch, got {dtype} expected {field2types[col]}"
                )
        # Deeper Type Check
        for (colname, colvals) in result.items():
            for val in colvals:
                type_errors = field2types[colname].validate(val)
                if type_errors:
                    raise Exception(
                        f"Column {colname} value {val} failed validation: {type_errors}"
                    )
        return result

    # Return the composite function
    return wrapper


# Metaclass that creates aggregate class
class AggregateMetaclass(type):
    def __new__(cls, name, bases, attrs):
        preaggregate_fn = None
        schema = None
        for attr_name, func in attrs.items():
            if attr_name == "preaggregate":
                preaggregate_fn = func
            elif attr_name == "schema":
                schema = func
        if preaggregate_fn is not None:
            attrs["preaggregate"] = wrap_preaggregate(
                cls, preaggregate_fn, schema
            )
            attrs["og_preaggregate"] = preaggregate_fn
        return super(AggregateMetaclass, cls).__new__(cls, name, bases, attrs)


KeyType = Union[str, List[str]]


@dataclasses.dataclass(frozen=True)
class AggregateType:
    key: KeyType
    timestamp: str

    def validate(self, agg: Any) -> List[Exception]:
        if self.key is None:
            raise Exception("key not provided")
        if self.timestamp is None:
            raise Exception("timestamp not provided")


class Aggregate(metaclass=AggregateMetaclass):
    name: str = None
    version: int = 1
    stream: str = None
    mode: str = "pandas"
    schema: Schema = None
    aggregate_type: AggregateType = None

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def register(cls, stub: FennelFeatureStoreStub) -> Status:
        if cls.windows is None:
            raise Exception("windows not provided")

        req = CreateAggregateRequest(
            name=cls.name,
            version=cls.version,
            stream=cls.stream,
            mode=cls.mode,
            aggregate_type=cls.aggregate_type.agg_func,
            preaggregate_function=cloudpickle.dumps(cls.og_preaggregate),
            function_source_code=inspect.getsource(cls.og_preaggregate),
            windows=[int(w.total_seconds()) for w in cls.windows],
            schema=cls.schema.to_proto(),
        )
        resp = stub.RegisterAggregate(req)
        return resp

    @classmethod
    def _validate_preaggregate(cls) -> List[Exception]:
        exceptions = []
        found_preaggregate = False
        class_methods = {
            name: func.__func__
            for name, func in cls.__dict__.items()
            if hasattr(func, "__func__")
        }
        for name, func in class_methods.items():
            if name[0] != "_" and name != "og_preaggregate":
                exceptions.append(
                    TypeError(
                        f"invalid method {name} found in aggregate "
                        f"class, only preaggregate is allowed"
                    )
                )
            if hasattr(func, "wrapped_function"):
                func = func.wrapped_function

            if func.__code__.co_argcount != 2:
                exceptions.append(
                    TypeError(
                        f"preaggregate function should take 2 arguments ( cls & df ) but got {func.__code__.co_argcount}"
                    )
                )
            found_preaggregate = True
        if not found_preaggregate:
            exceptions.append(
                Exception("preaggregate function not found in aggregate class")
            )
        return exceptions

    @classmethod
    def _validate(cls) -> List[Exception]:
        # Validate the schema
        exceptions = cls.schema.validate()
        if cls.name is None:
            exceptions.append(
                NameException(f"name not provided  {cls.__class__.__name__}")
            )
        if cls.stream is None:
            exceptions.append(
                Exception(
                    f"stream not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )

        # Validate the preaggregate function
        exceptions.extend(cls._validate_preaggregate())
        return exceptions


@dataclasses.dataclass(frozen=True)
class WindowBasedAggregate(AggregateType):
    value: str
    windows: List[Window] = None

    def validate(self, agg: Aggregate) -> List[Exception]:
        super().validate(agg)
        if self.windows is None:
            raise Exception("windows not provided")


@dataclasses.dataclass(frozen=True)
class Count(WindowBasedAggregate):
    agg_func = proto.AggregateType.COUNT


@dataclasses.dataclass(frozen=True)
class Sum(WindowBasedAggregate):
    agg_func = proto.AggregateType.SUM


@dataclasses.dataclass(frozen=True)
class Average(WindowBasedAggregate):
    agg_func = proto.AggregateType.AVG


@dataclasses.dataclass(frozen=True)
class Max(WindowBasedAggregate):
    agg_func = proto.AggregateType.MAX


@dataclasses.dataclass(frozen=True)
class Min(WindowBasedAggregate):
    agg_func = proto.AggregateType.MIN


@dataclasses.dataclass(frozen=True)
class KeyValue(AggregateType):
    value: str
    agg_func = proto.AggregateType.KEY_VALUE


@dataclasses.dataclass(frozen=True)
class Rate(AggregateType):
    agg_func: proto.AggregateType.RATE


@dataclasses.dataclass(frozen=True)
class TopK(AggregateType):
    item: KeyType
    score: str
    k: int
    agg_func: proto.AggregateType.TOPK
    update_frequency: int = 60

    def validate(self, agg: Aggregate) -> List[Exception]:
        super().validate(agg)
        if self.k is None:
            raise Exception("k not provided")
        if self.item is None:
            raise Exception("item not provided")
        if self.score is None:
            raise Exception("score not provided")
        if self.update_frequency is None:
            raise Exception("update_frequency not provided")


@dataclasses.dataclass(frozen=True)
class CF(AggregateType):
    context: KeyType
    weight: str
    agg_func: proto.AggregateType.CF

    def validate(self, agg: Aggregate) -> List[Exception]:
        if self.context is None:
            raise Exception("context not provided")
        if self.weight is None:
            raise Exception("weight not provided")


def depends_on(
    aggregates: Optional[List[Any]] = None,
    features: List[Any] = None,
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        if aggregates is not None:
            wrapper.depends_on_aggregates = aggregates
        else:
            wrapper.depends_on_aggregates = []

        if features is not None:
            wrapper.depends_on_features = features
        else:
            wrapper.depends_on_features = []
        wrapper.signature = inspect.signature(func)
        wrapper.wrapped_function = func
        wrapper.namespace = func.__globals__
        wrapper.func_name = func.__name__
        return wrapper

    return decorator
