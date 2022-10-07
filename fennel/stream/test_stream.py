import pickle

import pandas as pd
import pytest

from fennel.gen.stream_pb2 import CreateStreamRequest
from fennel.lib import Field, Schema, windows
from fennel.lib.schema import Array, Int, Map, String
from fennel.stream import MySQL, source, Stream
from fennel.test_lib import *

mysql_src = MySQL(
    name="mysql_psql_src",
    host="my-favourite-postgres.us-west-2.rds.amazonaws.com",
    db_name="some_database_name",
    username="admin",
    password="password",
)


class Actions(Stream):
    name = "actions"
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field("actor_id", dtype=Int, default=0),
            Field("target_id", dtype=Int, default=0),
            Field("action_type", dtype=String, default="love"),
            Field("timestamp", dtype=Int, default=0),
            Field(
                "metadata2",
                dtype=Array(Array(String)),
                default=[["a", "b", "c"], ["d", "e", "f"]],
            ),
            Field(
                "metadata3",
                dtype=Map(String, String),
                default={"yo": "hello"},
            ),
        )

    @source(src=mysql_src, table="actions")
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_StreamRegistration(grpc_stub):
    actions = Actions()
    workspace = InternalTestWorkspace(grpc_stub)
    responses = workspace.register_streams(actions)
    assert len(responses) == 1
    assert responses[0].code == 200
    create_stream_req = CreateStreamRequest()
    responses[0].details[0].Unpack(create_stream_req)
    assert len(create_stream_req.connectors) == 1
    create_connect_req = create_stream_req.connectors[0]
    populate = pickle.loads(create_connect_req.connector_function)
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    processed_df = populate(df)
    assert type(processed_df) == pd.DataFrame
    assert processed_df.shape == (3, 4)
    assert processed_df["actor_id"].tolist() == [1, 2, 5]
    assert processed_df["target_id"].tolist() == [1, 2, 5]
    assert processed_df["timestamp"].tolist() == [1.1, 2.1, 5.1]
    assert processed_df["action_type"].tolist() == ["like", "like", "like"]


class ActionsMultipleSources(Stream):
    name = "actions_multiple_sources"
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field("actor_id", dtype=Int, default=1),
            Field("target_id", dtype=Int, default=2),
            Field("action_type", dtype=Int, default=3),
            Field("timestamp", dtype=Int, default=0),
        )

    @source(src=mysql_src, table="like_actions")
    def populate_mysql_1(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]

    @source(src=mysql_src, table="")
    def populate_1(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "share"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]

    @source(src=mysql_src, table="comment_actions")
    def populate_actions(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "comment"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_StreamRegistration_MultipleSources(grpc_stub):
    workspace = InternalTestWorkspace(grpc_stub)
    workspace.register_streams(ActionsMultipleSources())
    # Duplicate registration should pass
    responses = workspace.register_streams(ActionsMultipleSources(), Actions())
    assert len(responses) == 2
    assert responses[0].code == 200
    create_stream_req = CreateStreamRequest()
    responses[0].details[0].Unpack(create_stream_req)
    assert len(create_stream_req.connectors) == 3
    create_connect_req = None
    for c in create_stream_req.connectors:
        if c.name == "populate_1":
            create_connect_req = c
    assert create_connect_req is not None
    populate = pickle.loads(create_connect_req.connector_function)
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    processed_df = populate(df)
    assert type(processed_df) == pd.DataFrame
    assert processed_df.shape == (1, 4)
    assert processed_df["actor_id"].tolist() == [3]
    assert processed_df["target_id"].tolist() == [3]
    assert processed_df["timestamp"].tolist() == [3.1]
    assert processed_df["action_type"].tolist() == ["share"]


class ActionsInvalidSchema(Stream):
    name = "actions"
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field("actor_id", dtype=int, default=0),
            Field("target_id", dtype=String, default=1),
            Field("action_type", dtype=Array(), default="love"),
        )

    @source(src=mysql_src, table="actions")
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


class ActionsInvalidSchema2(Stream):
    name = "actions"
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field("actor_id", dtype=int, default=0),
            Field("target_id", dtype=String, default=1),
            Field("action_type", dtype=Array(String), default="love"),
        )

    @source(src=mysql_src, table="actions")
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_InvalidStreamRegistration(grpc_stub):
    with pytest.raises(TypeError) as e:
        actions = ActionsInvalidSchema()
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_streams(actions)
    assert str(e.value) == "invalid array type: None"

    with pytest.raises(Exception) as e:
        actions = ActionsInvalidSchema2()
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_streams(actions)
    assert (
            str(e.value)
            == "[TypeError('Type for actor_id should be a Fennel Type object such as Int() and not a class such as "
               "Int/int'), "
               "TypeError('Expected default value for field target_id to be str, got 1'), TypeError('Expected "
               "default value for field action_type to be list, got love')]"
    )


class ActionsInvalidSource(Stream):
    name = "actions"
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field("actor_id", dtype=Int, default=0),
            Field("target_id", dtype=Int, default=0),
            Field("action_type", dtype=String, default="love"),
        )

    @source(src=mysql_src)
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_InvalidSource(grpc_stub):
    with pytest.raises(Exception) as e:
        actions = ActionsInvalidSource()
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_streams(actions)
    assert (
            str(e.value)
            == "[IncorrectSourceException('Incorrect source, table must be None since it supports only a single stream.')]"
    )
