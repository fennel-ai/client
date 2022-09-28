from abc import ABC
import pandas as pd
from typing import *
import inspect
from fennel.lib.schema import Schema, Field, FieldType

from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.utils import check_response, Singleton
from fennel.gen.status_pb2 import Status


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
    name = None
    version = 1
    stream: str = None
    mode = 'pandas'
    windows = None

    @classmethod
    def schema(cls) -> Schema:
        raise NotImplementedError()

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    def validate(self) -> List[Exception]:
        # Validate the schema
        # Cast to aggregate schema
        agg_schema = AggregateSchema(self.schema())
        exceptions = agg_schema.validate()
        # Validate the preprocess function
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.ismethod):
            if name not in ['preprocess', 'schema', 'version']:
                exceptions.append(TypeError(f'invalid method {name} found in aggregate class'))
            if name == 'preprocess' and func.__code__.co_argcount != 2:
                exceptions.append(
                    TypeError(
                        f'preprocess function should take 2 arguments ( self & df ) but got {func.__code__.co_argcount}'))
        return exceptions

    def register(self, stub: FennelFeatureStoreStub) -> Status:
        req = CreateAggregateRequest(
            name=self.name,
            version=self.version,
            stream=self.stream,
            mode=self.mode,
            windows=self.windows,
            schema=self.schema().to_proto(),
        )
        resp = stub.RegisterAggregate(req)
        check_response(resp)
        return resp


class Count(Aggregate, ABC):
    windows: List[int] = None


class Min(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Max(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class KeyValue(Aggregate, ABC):
    static = False

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Rate(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Average(Aggregate, ABC):
    windows: List[int] = None

    def _validate(self) -> List[Exception]:
        raise NotImplementedError()
