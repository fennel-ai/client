import inspect
from abc import ABC
from typing import *

import cloudpickle
import pandas as pd

import fennel.gen.aggregate_pb2 as proto
from fennel.errors import NameException
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.status_pb2 import Status
from fennel.lib.schema import FieldType, Schema
from fennel.lib.windows import Window
from fennel.utils import modify_aggregate_lookup, Singleton


class AggregateSchema(Schema):
    def __init__(self, schema: Schema):
        super().__init__(*schema.fields)

    def validate(self) -> List[Exception]:
        exceptions = self.validate_fields()
        key_fields = []
        value_fields = []
        timestamp_fields = []
        for field in self.fields:
            if field.field_type == FieldType.None_:
                exceptions.append(f'field {field.name} does not specify valid type - key, value, or timestamp')
            elif field.field_type == FieldType.Key:
                key_fields.append(field.name)
            elif field.field_type == FieldType.Value:
                value_fields.append(field.name)
            elif field.field_type == FieldType.Timestamp:
                timestamp_fields.append(field.name)
            else:
                exceptions.append(f'invalid field type: {field.type}')

        if len(key_fields) <= 0:
            exceptions.append("no field with type 'key' provided")
        if len(value_fields) <= 0:
            exceptions.append("no field with type 'value' provided")
        if len(timestamp_fields) != 1:
            exceptions.append(f'expected exactly one timestamp field but got: {len(timestamp_fields)} instead')

        return exceptions


class Aggregate(Singleton):
    name: str = None
    version: int = 1
    stream: str = None
    mode: str = 'pandas'
    windows: List[Window] = []

    def __init__(self, name: str, stream: str, windows: List[Window], version: int = 1,
                 mode: str = 'pandas', ):
        self.name = name
        self.version = version
        self.stream = stream
        self.mode = mode
        self.windows = windows

    @classmethod
    def wrap(cls, preprocess):
        if hasattr(preprocess, 'depends_on_aggregates'):
            depends_on_aggregates = preprocess.depends_on_aggregates
        else:
            depends_on_aggregates = []
        agg2names = {agg.instance().__class__.__name__: agg.instance().name for agg in depends_on_aggregates}
        mod_func, function_src_code = modify_aggregate_lookup(preprocess, agg2names)

        def outer(*args, **kwargs):
            result = mod_func(*args, **kwargs)
            # Validate the return value with the schema
            field2types = cls.schema().get_fields_and_types()
            for col, dtype in zip(result.columns, result.dtypes):
                if col not in field2types:
                    raise Exception("Column {} not in schema".format(col))
                if not field2types[col].type_check(dtype):
                    raise Exception(f'Column {col} type mismatch, got {dtype} expected {field2types[col]}')
            # Deeper Type Check
            for (colname, colvals) in result.items():
                for val in colvals:
                    type_errors = field2types[colname].validate(val)
                    if type_errors:
                        raise Exception(f'Column {colname} value {val} failed validation: {type_errors}')

            return result

        return outer, mod_func, function_src_code

    def __new__(cls, *args, **kwargs):
        instance = super(Aggregate, cls).__new__(cls, *args, **kwargs)
        # instance._instance = None
        for name, func in inspect.getmembers(instance.__class__, predicate=inspect.ismethod):
            if name == 'preprocess':
                instance.preprocess, instance.mod_preprocess, instance.function_src_code = cls.wrap(func)
        return instance

    @classmethod
    def schema(cls) -> Schema:
        raise NotImplementedError()

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    def _get_agg_type(self):
        raise NotImplementedError()

    def _validate_preprocess(self) -> List[Exception]:
        exceptions = []
        found_preprocess = False
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.ismethod):
            if name[0] != '_' and name not in ['preprocess', 'schema', 'version', 'instance', 'wrap']:
                exceptions.append(TypeError(f'invalid method {name} found in aggregate class'))
            if name != 'preprocess':
                continue
            if hasattr(func, 'wrapped_function'):
                func = func.wrapped_function
            if func.__code__.co_argcount != 2:
                exceptions.append(
                    TypeError(
                        f'preprocess function should take 2 arguments ( self & df ) but got {func.__code__.co_argcount}'))
            found_preprocess = True
        if not found_preprocess:
            exceptions.append(Exception(f'preprocess function not found in aggregate class'))
        return exceptions

    def validate(self) -> List[Exception]:
        # Validate the schema
        # Cast to aggregate schema
        agg_schema = AggregateSchema(self.schema())
        exceptions = agg_schema.validate()
        if self.name is None:
            exceptions.append(NameException(f'name not provided  {self.__class__.__name__}'))
        if self.stream is None:
            exceptions.append(Exception(f'stream not provided for aggregate  {self.__class__.__name__}'))

        # Validate the preprocess function
        exceptions.extend(self._validate_preprocess())

        if self.windows is None or len(self.windows) == 0:
            exceptions.append(Exception(f'windows not provided for aggregate  {self.__class__.__name__}'))
        return exceptions

    def register(self, stub: FennelFeatureStoreStub) -> Status:
        if self.windows is None:
            raise Exception("windows not provided")
        req = CreateAggregateRequest(
            name=self.name,
            version=self.version,
            stream=self.stream,
            mode=self.mode,
            aggregate_type=self._get_agg_type(),
            preprocess_function=cloudpickle.dumps(self.mod_preprocess),
            function_source_code=self.function_src_code,
            windows=[int(w.total_seconds()) for w in self.windows],
            schema=self.schema().to_proto(),
        )
        resp = stub.RegisterAggregate(req)
        return resp


def depends_on(
        aggregates: List[Aggregate] = [],
        features: List[Any] = [],
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper.depends_on_aggregates = aggregates
        wrapper.depends_on_features = features
        wrapper.signature = inspect.signature(func)
        wrapper.wrapped_function = func
        wrapper.namespace = func.__globals__
        wrapper.func_name = func.__name__
        return wrapper

    return decorator


class Count(Aggregate, ABC):
    windows: List[int] = None

    def _get_agg_type(self):
        return proto.AggregateType.COUNT

    def validate(self) -> List[Exception]:
        return super().validate()


class Min(Aggregate, ABC):
    windows: List[int] = None

    def _get_agg_type(self):
        return proto.AggregateType.MIN

    def validate(self) -> List[Exception]:
        return super().validate()


class Max(Aggregate, ABC):
    windows: List[int] = None

    def _get_agg_type(self):
        return proto.AggregateType.MAX

    def validate(self) -> List[Exception]:
        return super().validate()


class KeyValue(Aggregate, ABC):
    static = False

    def _get_agg_type(self):
        return proto.AggregateType.KEY_VALUE

    def validate(self) -> List[Exception]:
        return super().validate()


class Rate(Aggregate, ABC):
    windows: List[int] = None

    def _get_agg_type(self):
        return proto.AggregateType.RATE

    def validate(self) -> List[Exception]:
        return super().validate()


class Average(Aggregate, ABC):
    windows: List[int] = None

    def _get_agg_type(self):
        return proto.AggregateType.AVG

    def validate(self) -> List[Exception]:
        return super().validate()
