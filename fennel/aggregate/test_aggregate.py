import pickle

import pandas as pd
import pytest

from fennel.aggregate import Count
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.lib import Field, Schema, windows
from fennel.lib.schema import Double, FieldType, Int, String
from fennel.test_lib import *


class UserLikeCount(Count):
    name = "TestUserLikeCount"
    stream = "Actions"
    windows = [windows.DAY * 7, windows.DAY * 28]

    schema = Schema([
        Field("actor_id", Int, 0, field_type=FieldType.Key),
        Field("target_id", Int, 0, field_type=FieldType.Value),
        Field("timestamp", Double, 0.0, field_type=FieldType.Timestamp)]
    )

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "like"].copy()
        filtered_df["actor_id"].rename("uid")
        filtered_df.drop(columns=["action_type"], inplace=True)
        return filtered_df


def test_AggregateRegistration(grpc_stub):
    workspace = InternalTestWorkspace(grpc_stub)
    responses = workspace.register_aggregates(UserLikeCount)
    assert len(responses) == 1
    assert responses[0].code == 200
    create_agg = CreateAggregateRequest()
    responses[0].details[0].Unpack(create_agg)
    preprocess = pickle.loads(create_agg.preaggregate_function)
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    processed_df = preprocess(df)
    assert type(processed_df) == pd.DataFrame
    assert processed_df.shape == (3, 3)


class UserLikeCountInvalidSchema(Count):
    name = "TestUserLikeCount"
    stream = "Actions"
    windows = [windows.DAY * 7, windows.DAY * 28]
    schema = Schema(
        [Field("uid", int, 0, field_type=FieldType.Key),
         Field("count", int, 0, field_type=FieldType.Value),
         Field("timestamp", int, 0, field_type=FieldType.Timestamp)]
    )

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        df["actor_id"].rename("uid")
        df = df.drop("action_type")
        return df


def test_InvalidSchemaAggregateRegistration(grpc_stub):
    with pytest.raises(Exception) as e:
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_aggregates(UserLikeCountInvalidSchema)

    assert (
            str(e.value)
            == "[TypeError('Type for uid should be a Fennel Type object such as Int() and not a class such as Int/int'), "
               "TypeError('Type for count should be a Fennel Type object such as Int() and not a class such as Int/int'), "
               "TypeError('Type for timestamp should be a Fennel Type object such as Int() and not a class such as Int/int')]"
    )


class UserLikeCountInvalidProcessingFunction(Count):
    name = "TestUserLikeCount"
    stream = "Actions"
    windows = [windows.DAY * 7, windows.DAY * 28]
    schema = Schema(
        [Field("uid", String, "aditya", field_type=FieldType.Key),
         Field("count", Int, 12, field_type=FieldType.Value),
         Field("timestamp", Int, 1234, field_type=FieldType.Timestamp)],
    )

    @classmethod
    def preaggregate(
            cls, df: pd.DataFrame, user_df: pd.DataFrame
    ) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        df["actor_id"].rename("uid")
        df = df.drop("action_type")
        return df


def test_InvalidProcessingFunctionAggregateRegistration(grpc_stub):
    with pytest.raises(Exception) as e:
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_aggregates(UserLikeCountInvalidProcessingFunction)
    assert (
            str(e.value)
            == "[TypeError('preaggregate function should take 2 arguments ( "
               "cls & df ) but got 3')]"
    )


class UserLikeCountInvalidProcessingFunction2(Count):
    name = "TestUserLikeCount"
    stream = "Actions"
    windows = [windows.DAY * 7, windows.DAY * 28]
    schema = Schema([
        Field("uid", String, "aditya", field_type=FieldType.Key),
        Field("count", Int, 12, field_type=FieldType.Value),
        Field("timestamp", Int, 1234, field_type=FieldType.Timestamp)]
    )

    @classmethod
    def preprocess2(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        df["actor_id"].rename("uid")
        df = df.drop("action_type")
        return df


def test_InvalidProcessingFunctionAggregateRegistration2(grpc_stub):
    with pytest.raises(Exception) as e:
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_aggregates(UserLikeCountInvalidProcessingFunction2)
    assert (str(e.value)
            == "[TypeError('invalid method preprocess2 found in aggregate "
               "class, only preaggregate is allowed')]")
