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
class AggregateFunction:
    key: KeyType
    timestamp: str

    def validate(self, agg: Any) -> List[Exception]:
        exceptions = []
        if self.key is None:
            exceptions.append(Exception("key not provided"))
        if self.timestamp is None:
            exceptions.append(Exception("timestamp not provided"))
        return exceptions

    def to_proto(self) -> proto.AggregateFunction:
        raise NotImplementedError


class Aggregate(metaclass=AggregateMetaclass):
    name: str = None
    version: int = 0
    stream: str = None
    mode: str = "pandas"
    schema: Schema = None
    aggregate_type: AggregateFunction = None

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def register(cls, stub: FennelFeatureStoreStub) -> Status:
        req = CreateAggregateRequest(
            name=cls.name,
            version=cls.version,
            stream=cls.stream,
            mode=cls.mode,
            aggregate_type=cls.aggregate_type.to_proto(),
            agg_cls=cloudpickle.dumps(cls),
            function_source_code=inspect.getsource(cls.og_preaggregate),
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
        exceptions.extend(cls.aggregate_type.validate(cls))
        # Validate the aggregate schema contains a timestamp field
        exceptions.extend(cls.schema.check_timestamp_field_exists())
        # Validate the preaggregate function
        exceptions.extend(cls._validate_preaggregate())
        return exceptions


@dataclasses.dataclass(frozen=True)
class WindowBasedAggregate(AggregateFunction):
    value: str
    windows: List[Window] = None

    def validate(self, agg: Aggregate) -> List[Exception]:
        exceptions = super().validate(agg)
        if self.windows is None:
            exceptions.append(Exception("windows not provided"))
        return exceptions

    def to_proto(self) -> proto.AggregateFunction:
        return proto.AggregateType(
            function=self.agg_func,
            key_fields=self.key if isinstance(self.key, list) else [self.key],
            timestamp_field=self.timestamp,
            window_config=proto.WindowConfig(
                value_field=self.value,
                windows=[int(w.total_seconds()) for w in self.windows],
            ),
        )


@dataclasses.dataclass(frozen=True)
class Count(WindowBasedAggregate):
    agg_func = proto.AggregateFunction.COUNT


@dataclasses.dataclass(frozen=True)
class Sum(WindowBasedAggregate):
    agg_func = proto.AggregateFunction.SUM


@dataclasses.dataclass(frozen=True)
class Average(WindowBasedAggregate):
    agg_func = proto.AggregateFunction.AVG


@dataclasses.dataclass(frozen=True)
class Max(WindowBasedAggregate):
    agg_func = proto.AggregateFunction.MAX


@dataclasses.dataclass(frozen=True)
class Min(WindowBasedAggregate):
    agg_func = proto.AggregateFunction.MIN


@dataclasses.dataclass(frozen=True)
class KeyValue(AggregateFunction):
    value: str
    agg_func = proto.AggregateFunction.KEY_VALUE

    def to_proto(self) -> proto.AggregateType:
        return proto.AggregateType(
            function=self.agg_func,
            key_fields=self.key if isinstance(self.key, list) else [self.key],
            timestamp_field=self.timestamp,
            key_value_config=proto.KeyValueConfig(
                value_field=self.value,
            ),
        )


@dataclasses.dataclass(frozen=True)
class Rate(AggregateFunction):
    agg_func: proto.AggregateFunction.RATE


@dataclasses.dataclass(frozen=True)
class TopK(AggregateFunction):
    item: KeyType
    score: str
    k: int
    agg_func: proto.AggregateFunction.TOPK
    update_frequency: int = 60

    def validate(self, agg: Aggregate) -> List[Exception]:
        exceptions = super().validate(agg)
        if self.k is None:
            exceptions.append(Exception("k not provided"))
        if self.item is None:
            exceptions.append(Exception("item not provided"))
        if self.score is None:
            exceptions.append(Exception("score not provided"))
        if self.update_frequency is None:
            exceptions.append(Exception("update_frequency not provided"))
        return exceptions


@dataclasses.dataclass(frozen=True)
class CF(AggregateFunction):
    context: KeyType
    weight: str
    agg_func: proto.AggregateFunction.CF

    def validate(self, agg: Aggregate) -> List[Exception]:
        exceptions = super().validate(agg)
        if self.context is None:
            exceptions.append(Exception("context not provided"))
        if self.weight is None:
            exceptions.append(Exception("weight not provided"))
        return exceptions


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
