from abc import ABC
import pandas as pd
from typing import *
import inspect
from fennel.lib.schema import Schema, Field, FieldType
import cloudpickle

from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.utils import check_response, Singleton
from fennel.gen.status_pb2 import Status
import fennel.gen.aggregate_pb2 as proto
from fennel.errors import NameException
from fennel.lib.windows import Window


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


def aggregate_lookup(agg_name: str, **kwargs):
    print("Aggregate", agg_name, " lookup will be patched")
    pass


class Aggregate(Singleton):
    name: str = None
    version: int = 1
    stream: str = None
    mode: str = 'pandas'
    windows: List[Window] = []

    @classmethod
    def schema(cls) -> Schema:
        raise NotImplementedError()

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    def _get_agg_type(self):
        raise NotImplementedError()

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
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.ismethod):
            if name[0] != '_' and name not in ['preprocess', 'schema', 'version']:
                exceptions.append(TypeError(f'invalid method {name} found in aggregate class'))
            if name == 'preprocess' and func.__code__.co_argcount != 2:
                exceptions.append(
                    TypeError(
                        f'preprocess function should take 2 arguments ( self & df ) but got {func.__code__.co_argcount}'))
        return exceptions

    def _get_pickle_preprocess_function(self):
        for name, func in inspect.getmembers(self.__class__, predicate=inspect.ismethod):
            if name == 'preprocess':
                return cloudpickle.dumps(func)

    def register(self, stub: FennelFeatureStoreStub) -> Status:
        req = CreateAggregateRequest(
            name=self.name,
            version=self.version,
            stream=self.stream,
            mode=self.mode,
            aggregate_type=self._get_agg_type(),
            preprocess_function=self._get_pickle_preprocess_function(),
            windows=[int(w.total_seconds()) for w in self.windows],
            schema=self.schema().to_proto(),
        )
        resp = stub.RegisterAggregate(req)
        return resp


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

    @classmethod
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
