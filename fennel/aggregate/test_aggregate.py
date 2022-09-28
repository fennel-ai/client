import pytest

from typing import List, Optional, Union
from fennel.lib import (Field, Schema, windows)
from fennel.aggregate import (Aggregate, Count, Min, Max, KeyValue)
from fennel.lib.schema import (
    Int,
    Double,
    Bool,
    Map,
    Array,
    String,
    FieldType
)
import pandas as pd
from fennel.lib.windows import Window
from fennel.test_lib import *


class UserLikeCount(Count):
    def __init__(self, stream, windows: List[Window]):
        self.stream = stream

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', Int(), 0, field_type=FieldType.Key),
            Field('count', Int(), 0, field_type=FieldType.Value),
            Field('timestamp', Int(), 0, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        df['actor_id'].rename('uid')
        df = df.drop('action_type')
        return df


def test_AggregateRegistration(grpc_stub):
    agg = UserLikeCount('actions', [windows.DAY * 7, windows.DAY * 28])
    workspace = WorkspaceTest(grpc_stub)
    workspace.register_aggregates(agg)


class UserLikeCountInvalidSchema(Count):
    def __init__(self, stream, windows: List[Window]):
        self.stream = stream

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', int, 0, field_type=FieldType.Key),
            Field('count', int, 0, field_type=FieldType.Value),
            Field('timestamp', int, 0, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        df['actor_id'].rename('uid')
        df = df.drop('action_type')
        return df


def test_InvalidSchemaAggregateRegistration(grpc_stub):
    with pytest.raises(Exception) as e:
        agg = UserLikeCountInvalidSchema('actions', [windows.DAY * 7, windows.DAY * 28])
        workspace = WorkspaceTest(grpc_stub)
        workspace.register_aggregates(agg)
    assert str(
        e.value) == "[TypeError('Type should be a Fennel Type object such as Int() and not a class such as Int/int'), " \
                    "TypeError('Type should be a Fennel Type object such as Int() and not a class such as Int/int'), " \
                    "TypeError('Type should be a Fennel Type object such as Int() and not a class such as Int/int')]"


class UserLikeCountInvalidProcessingFunction(Count):
    def __init__(self, stream, windows: List[Window]):
        self.stream = stream

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', String(), "aditya", field_type=FieldType.Key),
            Field('count', Int(), 12, field_type=FieldType.Value),
            Field('timestamp', Int(), 1234, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame, user_df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        df['actor_id'].rename('uid')
        df = df.drop('action_type')
        return df


def test_InvalidProcessingFunctionAggregateRegistration(grpc_stub):
    with pytest.raises(Exception) as e:
        agg = UserLikeCountInvalidProcessingFunction('actions', [windows.DAY * 7, windows.DAY * 28])
        workspace = WorkspaceTest(grpc_stub)
        workspace.register_aggregates(agg)
    assert str(e.value) == "[TypeError('preprocess function should take 2 arguments ( self & df ) but got 3')]"


class UserLikeCountInvalidProcessingFunction2(Count):
    def __init__(self, stream, windows: List[Window]):
        self.stream = stream

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', String(), "aditya", field_type=FieldType.Key),
            Field('count', Int(), 12, field_type=FieldType.Value),
            Field('timestamp', Int(), 1234, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess2(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        df['actor_id'].rename('uid')
        df = df.drop('action_type')
        return df


def test_InvalidProcessingFunctionAggregateRegistration2(grpc_stub):
    with pytest.raises(Exception) as e:
        agg = UserLikeCountInvalidProcessingFunction2('actions', [windows.DAY * 7, windows.DAY * 28])
        workspace = WorkspaceTest(grpc_stub)
        workspace.register_aggregates(agg)
    assert str(e.value) == "[TypeError('invalid method preprocess2 found in aggregate class')]"
