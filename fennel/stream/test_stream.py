import pickle

import pandas as pd
import pytest

from fennel.gen.stream_pb2 import CreateStreamRequest
from fennel.lib import Field, Schema, windows
from fennel.lib.schema import Array, Int, Map, Now, String, Timestamp
from fennel.stream import MySQL, populator, Stream
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
    schema = Schema(
        [
            Field("actor_id", dtype=Int, default=0),
            Field("target_id", dtype=Int, default=0),
            Field("action_type", dtype=String, default="love"),
            Field("timestamp", dtype=Timestamp, default=Now),
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
        ],
    )

    @classmethod
    @populator(source=mysql_src, table="actions")
    def populate(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_StreamRegistration(grpc_stub):
    workspace = InternalTestWorkspace(grpc_stub)
    responses = workspace.register_streams(Actions)
    assert len(responses) == 1
    assert responses[0].code == 200
    create_stream_req = CreateStreamRequest()
    responses[0].details[0].Unpack(create_stream_req)
    assert len(create_stream_req.connectors) == 1
    create_connect_req = create_stream_req.connectors[0]
    populate = pickle.loads(create_connect_req.connector_function)
    now = pd.Timestamp.now()
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [now, now, now, now, now],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    stream = pickle.loads(create_stream_req.stream_cls)
    processed_df = populate(stream, df)
    assert type(processed_df) == pd.DataFrame
    assert processed_df.shape == (3, 4)
    assert processed_df["actor_id"].tolist() == [1, 2, 5]
    assert processed_df["target_id"].tolist() == [1, 2, 5]
    assert processed_df["timestamp"].tolist() == [now, now, now]
    assert processed_df["action_type"].tolist() == ["like", "like", "like"]


class ActionsMultipleSources(Stream):
    name = "actions_multiple_sources"
    retention = windows.DAY * 14
    schema = Schema(
        [
            Field("actor_id", dtype=Int, default=1),
            Field("target_id", dtype=Int, default=2),
            Field("action_type", dtype=String, default="action"),
            Field("timestamp", dtype=Timestamp, default=pd.Timestamp.now()),
        ],
    )

    @classmethod
    @populator(source=mysql_src, table="like_actions")
    def populate_mysql_1(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]

    @classmethod
    @populator(source=mysql_src, table="")
    def populate_1(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "share"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]

    @classmethod
    @populator(source=mysql_src, table="comment_actions")
    def populate_actions(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "comment"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_StreamRegistration_MultipleSources(grpc_stub):
    workspace = InternalTestWorkspace(grpc_stub)
    workspace.register_streams(ActionsMultipleSources)
    # Duplicate registration should pass
    responses = workspace.register_streams(ActionsMultipleSources, Actions)
    assert len(responses) == 2
    assert responses[0].code == 200
    create_stream_req = CreateStreamRequest()
    responses[0].details[0].Unpack(create_stream_req)
    assert len(create_stream_req.connectors) == 3
    create_connect_req = None
    for c in create_stream_req.connectors:
        if c.name == "actions_multiple_sources:populate_1":
            create_connect_req = c
    assert create_connect_req is not None
    populate = pickle.loads(create_connect_req.connector_function)
    now = pd.Timestamp.now()
    yest = now - pd.Timedelta(days=1)
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [now, yest, now, yest, now],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    stream = pickle.loads(create_stream_req.stream_cls)
    processed_df = populate(stream, df)
    assert type(processed_df) == pd.DataFrame
    assert processed_df.shape == (1, 4)
    assert processed_df["actor_id"].tolist() == [3]
    assert processed_df["target_id"].tolist() == [3]
    assert processed_df["timestamp"].tolist() == [now]
    assert processed_df["action_type"].tolist() == ["share"]


class ActionsInvalidSchema2(Stream):
    name = "actions"
    retention = windows.DAY * 14
    schema = Schema(
        [
            Field("actor_id", dtype=int, default=0),
            Field("target_id", dtype=String, default=1),
            Field("action_type", dtype=Array(String), default="love"),
            Field("timestamp", dtype=Timestamp, default=Now),
        ],
    )

    @classmethod
    @populator(source=mysql_src, table="actions")
    def populate(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_InvalidStreamRegistration(grpc_stub):
    with pytest.raises(TypeError) as e:

        class ActionsInvalidSchema(Stream):
            name = "actions"
            retention = windows.DAY * 14
            schema = Schema(
                [
                    Field("actor_id", dtype=int, default=0),
                    Field("target_id", dtype=String, default=1),
                    Field("action_type", dtype=Array(), default="love"),
                ],
            )

            @classmethod
            @populator(source=mysql_src, table="actions")
            def populate(cls, df: pd.DataFrame) -> pd.DataFrame:
                df = df[df["action_type"] == "like"]
                return df[["actor_id", "target_id", "action_type", "timestamp"]]

    assert str(e.value) == "invalid array type: None"

    with pytest.raises(Exception) as e:
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_streams(ActionsInvalidSchema2)
    assert (
        str(e.value) == "[TypeError('Type for actor_id should be a "
        "Fennel Type object "
        "such as Int() and not a class such as Int/int'), "
        "TypeError('Expected default value for field "
        "target_id to be str, got 1'), TypeError('Expected "
        "default value for field action_type to be list, "
        "got love')]"
    )


class ActionsInvalidSource(Stream):
    name = "actions"
    retention = windows.DAY * 14
    schema = Schema(
        [
            Field("actor_id", dtype=Int, default=0),
            Field("target_id", dtype=Int, default=0),
            Field("action_type", dtype=String, default="love"),
            Field(
                "timestamp", dtype=Timestamp, default=pd.Timestamp("2020-01-01")
            ),
        ],
    )

    @classmethod
    @populator(source=mysql_src)
    def populate(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        return df[["actor_id", "target_id", "action_type", "timestamp"]]


def test_InvalidSource(grpc_stub):
    with pytest.raises(Exception) as e:
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_streams(ActionsInvalidSource)
    assert (
        str(e.value)
        == "table must be provided since it supports multiple streams/tables"
    )
