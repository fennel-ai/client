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
from fennel.workspace import WorkspaceTest


# ----------------------Setup Fixtures------------------------------------------------

@pytest.fixture(scope='module')
def grpc_add_to_server():
    from fennel.gen.services_pb2_grpc import add_FennelFeatureStoreServicer_to_server
    return add_FennelFeatureStoreServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    return Servicer()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
    return FennelFeatureStoreStub


class Servicer(FennelFeatureStoreServicer):
    def RegisterSource(self, request, context) -> Status:
        return Status(code=200, message=f'test')

    def RegisterConnector(self, request, context) -> Status:
        return Status(code=200, message=f'test')


# ----------------------End Set Up------------------------------------------------

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

    with pytest.raises(TypeError):
        @source()
        def populate2(self, df: pd.DataFrame) -> pd.DataFrame:
            df = df[df['action_type'] == 'like']
            return df[['actor_id', 'target_id', 'action_type', 'timestamp']]

    with pytest.raises(IncorrectSourceException):
        @source(src=mysql_src)
        def populate2(self, df: pd.DataFrame) -> pd.DataFrame:
            df = df[df['action_type'] == 'like']
            return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


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
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'share_actions']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]

    @source(src=mysql_src, table='comment_actions')
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['action_type'] == 'comment']
        return df[['actor_id', 'target_id', 'action_type', 'timestamp']]


def test_StreamRegistration_MultipleSources(grpc_stub):
    workspace = WorkspaceTest(grpc_stub)
    workspace.register_streams(ActionsMultipleSources())
    # Duplicate registration should pass
    workspace.register_streams(ActionsMultipleSources(), Actions())
