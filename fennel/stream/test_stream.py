import pandas as pd
import pytest

from fennel.errors import IncorrectSourceException
from fennel.gen.services_pb2_grpc import FennelFeatureStoreServicer
from fennel.gen.status_pb2 import Status
from fennel.lib import (Field, Schema, windows)
from fennel.lib.schema import (
    Int,
    Map,
    Array,
    String,
)
from fennel.stream import (MySQL, source, Stream)
from fennel.test_lib import *

mysql_src = MySQL(
    name='mysql_psql_src',
    host="my-favourite-postgres.us-west-2.rds.amazonaws.com",
    db_name="some_database_name",
    username="admin",
    password="password",
)


class Actions(Stream):
    name = 'actions'
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', dtype=Int(), default=0),
            Field('target_id', dtype=Int(), default=0),
            Field('action_type', dtype=String(), default="love"),
            Field('timestamp', dtype=Int(), default=0),
            Field('metadata2', dtype=Array(Array(String())), default=[["a", "b", "c"], ["d", "e", "f"]]),
            Field('metadata3', dtype=Map(String(), String()), default={"yo": 'hello'}),
        )

    @source(src=mysql_src, table='actions')
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


def test_StreamRegistration(grpc_stub):
    actions = Actions()
    workspace = WorkspaceTest(grpc_stub)
    workspace.register_streams(actions)


class ActionsMultipleSources(Stream):
    name = 'actions_multiple_sources'
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', dtype=Int(), default=1),
            Field('target_id', dtype=Int(), default=2),
            Field('action_type', dtype=Int(), default=3),
            Field('timestamp', dtype=Int(), default=0),
        )

    @source(src=mysql_src, table='like_actions')
    def populate_mysql_1(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]

    @source(src=mysql_src, table='')
    def populate_1(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'share_actions']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]

    @source(src=mysql_src, table='comment_actions')
    def populate_actions(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'comment']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


def test_StreamRegistration_MultipleSources(grpc_stub):
    workspace = WorkspaceTest(grpc_stub)
    workspace.register_streams(ActionsMultipleSources())
    # Duplicate registration should pass
    workspace.register_streams(ActionsMultipleSources(), Actions())


class ActionsInvalidSchema(Stream):
    name = 'actions'
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', dtype=int, default=0),
            Field('target_id', dtype=String(), default=1),
            Field('action_type', dtype=Array(), default="love"),
        )

    @source(src=mysql_src, table='actions')
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


class ActionsInvalidSchema2(Stream):
    name = 'actions'
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', dtype=int, default=0),
            Field('target_id', dtype=String(), default=1),
            Field('action_type', dtype=Array(String()), default="love"),
        )

    @source(src=mysql_src, table='actions')
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


def test_InvalidStreamRegistration(grpc_stub):
    with pytest.raises(TypeError) as e:
        actions = ActionsInvalidSchema()
        workspace = WorkspaceTest(grpc_stub)
        workspace.register_streams(actions)
    assert str(e.value) == "invalid array type: None"

    with pytest.raises(Exception) as e:
        actions = ActionsInvalidSchema2()
        workspace = WorkspaceTest(grpc_stub)
        workspace.register_streams(actions)
    assert str(
        e.value) == "[TypeError('Type should be a Fennel Type object such as Int() and not a class such as " \
                    "Int/int'), " \
                    "TypeError('Expected default value for field target_id to be str, got 1'), TypeError('Expected " \
                    "default value for field action_type to be list, got love')]"


class ActionsInvalidSource(Stream):
    name = 'actions'
    retention = windows.DAY * 14

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', dtype=Int(), default=0),
            Field('target_id', dtype=Int(), default=0),
            Field('action_type', dtype=String(), default="love"),
        )

    @source(src=mysql_src)
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'like']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


def test_InvalidSource(grpc_stub):
    with pytest.raises(Exception) as e:
        actions = ActionsInvalidSource()
        workspace = WorkspaceTest(grpc_stub)
        workspace.register_streams(actions)
    assert str(
        e.value) == "[IncorrectSourceException('Incorrect source, table must be None since it supports only a single stream.')]"
