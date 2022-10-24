import pickle

import pandas as pd
import pytest

from fennel.aggregate import Aggregate, Count
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.lib import Field, Schema, windows
from fennel.lib.schema import Int, Now, String, Timestamp
from fennel.test_lib import *


class UserLikeCount(Aggregate):
    name = "TestUserLikeCount"
    stream = "Actions"

    schema = Schema(
        [
            Field(
                "actor_id",
                dtype=Int,
                default=0,
            ),
            Field(
                "target_id",
                dtype=Int,
                default=0,
            ),
            Field(
                "timestamp",
                dtype=Timestamp,
                default=Now,
            ),
        ]
    )

    aggregation = Count(
        key="actor_id",
        value="target_id",
        timestamp="timestamp",
        windows=[windows.DAY * 7],
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
    assert responses[0].code == 0
    create_agg = CreateAggregateRequest()
    responses[0].details[0].Unpack(create_agg)
    agg_cls = pickle.loads(create_agg.agg_cls)
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    # Schema check is done in the preaggregate function not in
    # og_preaggregate. Since that will be done by the server.
    processed_df = agg_cls.og_preaggregate(df)
    assert type(processed_df) == pd.DataFrame
    assert processed_df.shape == (3, 3)


class UserLikeCountInvalidSchema(Aggregate):
    name = "TestUserLikeCount"
    stream = "Actions"
    schema = Schema(
        [
            Field(
                "uid",
                dtype=int,
                default=0,
            ),
            Field(
                "count",
                dtype=int,
                default=0,
            ),
            Field(
                "timestamp1",
                dtype=int,
                default=0,
            ),
        ]
    )
    aggregation = Count(
        key="actor_id",
        value="target_id",
        windows=[
            windows.DAY * 7,
        ],
        timestamp="timestamp",
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
        "TypeError('Type for timestamp1 should be a Fennel Type object such as Int() and not a class such as Int/int'), "
        "Exception('No timestamp field provided')]"
    )


class UserLikeCountInvalidProcessingFunction(Aggregate):
    name = "TestUserLikeCount"
    stream = "Actions"
    schema = Schema(
        [
            Field(
                "uid",
                dtype=String,
                default="aditya",
            ),
            Field(
                "count",
                dtype=Int,
                default=12,
            ),
            Field(
                "timestamp",
                dtype=Timestamp,
                default=Now,
            ),
        ],
    )
    aggregation = Count(
        key="actor_id",
        value="target_id",
        timestamp="timestamp",
        windows=[
            windows.DAY * 7,
        ],
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


class UserLikeCountInvalidProcessingFunction2(Aggregate):
    name = "TestUserLikeCount"
    stream = "Actions"
    schema = Schema(
        [
            Field(
                "uid",
                dtype=String,
                default="aditya",
            ),
            Field("count", dtype=Int, default=12),
            Field("timestamp", dtype=Timestamp, default=Now),
        ]
    )
    aggregation = Count(
        key="actor_id",
        value="target_id",
        timestamp="timestamp",
        windows=[
            windows.DAY * 7,
        ],
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
    assert (
        str(e.value)
        == "[TypeError('invalid method preprocess2 found in aggregate "
        "class, only preaggregate is allowed')]"
    )
