import pandas as pd
import pytest

from fennel.errors import IncorrectSourceException
from fennel.gen.services_pb2_grpc import FennelFeatureStoreServicer
from fennel.gen.status_pb2 import Status
from fennel.lib import (Field, Schema, windows)
from typing import List, Optional, Union
from fennel.gen.aggregate_pb2 import CreateAggregateRequest

from fennel.lib.schema import (
    Int,
    Map,
    Array,
    Double,
    Bool,
    String,
)
from fennel.stream import (MySQL, source, Stream)
from fennel.test_lib import *
from fennel.aggregate import (Aggregate, Count, Min, Max, KeyValue, depends_on)
from fennel.lib.windows import Window
from fennel.lib.schema import (
    Int,
    Double,
    Bool,
    Map,
    Array,
    String,
    FieldType
)
from fennel.feature import aggregate_lookup

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
            Field('gender', dtype=Bool(), default=False),
            Field('timestamp', dtype=Double(), default=0.0),
            Field('random_array', dtype=Array(Array(String())), default=[["a", "b", "c"], ["d", "e", "f"]]),
            Field('metadata', dtype=Map(String(), Array(Int())), default=[["a", "b", "c"], ["d", "e", "f"]]),
        )

    @source(src=mysql_src, table='actions')
    def populate(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['action_type'] == 'like'].copy()
        filtered_df['metadata'] = filtered_df['target_id'].apply(lambda x: {str(x): [x, x + 1, x + 2]})
        filtered_df['random_array'] = filtered_df['target_id'].apply(lambda x: [[str(x), str(x + 1), str(x + 2)]])
        return filtered_df[['actor_id', 'target_id', 'action_type', 'timestamp', 'random_array', 'metadata', 'gender']]

    @source(src=mysql_src, table='actions')
    def invalid_populate(self, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['action_type'] == 'like'].copy()
        filtered_df.loc[:, "random_array"] = filtered_df['target_id'].apply(
            lambda x: {str(x): [x, x + 1, x + 2]}).copy()
        filtered_df.loc[:, 'metadata'] = filtered_df['target_id'].apply(lambda x: [[str(x), str(x + 1), str(x + 2)]])
        return filtered_df[['actor_id', 'target_id', 'action_type', 'timestamp', 'random_array', 'metadata', 'gender']]


############################################################################################################
# Stream Tests
############################################################################################################

def test_StreamProcess():
    actions = Actions()
    df = pd.DataFrame(
        {
            'actor_id': [1, 2, 3, 4, 5],
            'target_id': [1, 2, 3, 4, 5],
            'action_type': ['like', 'like', 'share', 'comment', 'like'],
            'gender': [True, False, True, False, True],
            'timestamp': [1.1, 2.1, 3.1, 4.1, 5.1],
        })
    processed_df = actions.populate(df)
    assert processed_df.shape == (3, 7)


def test_InvalidPopulate_StreamProcess():
    actions = Actions()
    df = pd.DataFrame(
        {
            'actor_id': [1, 2, 3, 4, 5],
            'target_id': [1, 2, 3, 4, 5],
            'action_type': ['like', 'like', 'share', 'comment', 'like'],
            'gender': [True, False, True, False, True],
            'timestamp': [1.1, 2.1, 3.1, 4.1, 5.1],
        })
    with pytest.raises(Exception) as e:
        actions.invalid_populate(df)
    assert str(
        e.value) == """Column random_array value {'1': [1, 2, 3]} failed validation: [TypeError("Expected list, got <class 'dict'>")]"""


############################################################################################################
# Aggregate Tests
############################################################################################################

class UserLikeCount(Count):
    def __init__(self, stream, windows: List[Window]):
        super().__init__('TestUserLikeCount', stream, windows)

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('actor_id', Int(), 0, field_type=FieldType.Key),
            Field('target_id', Int(), 0, field_type=FieldType.Value),
            Field('timestamp', Int(), 0, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['action_type'] == 'like'].copy()
        filtered_df['actor_id'].rename('uid')
        filtered_df.drop(columns=['action_type'], inplace=True)
        return filtered_df


def test_AggregatePreprocess(grpc_stub):
    agg = UserLikeCount('actions', [windows.DAY * 7, windows.DAY * 28])
    workspace = WorkspaceTest(grpc_stub)
    responses = workspace.register_aggregates(agg)

    assert len(responses) == 1
    assert responses[0].code == 200
    create_agg = CreateAggregateRequest()
    responses[0].details[0].Unpack(create_agg)
    df = pd.DataFrame(
        {
            'actor_id': [1, 2, 3, 4, 5],
            'target_id': [1, 2, 3, 4, 5],
            'timestamp': [1.1, 2.1, 3.1, 4.1, 5.1],
            'action_type': ['like', 'like', 'share', 'comment', 'like'],
        })
    processed_df = agg.preprocess(df)
    assert processed_df.shape == (3, 3)
    assert processed_df['actor_id'].tolist() == [1, 2, 5]
    assert processed_df['timestamp'].tolist() == [1.1, 2.1, 5.1]
    assert processed_df['target_id'].tolist() == [1, 2, 5]


class UserGenderKVAgg(KeyValue):
    def __init__(self, stream, windows: List[Window]):
        super().__init__('TestUserGenderKVAgg', stream, windows)

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('uid', Int(), 0, field_type=FieldType.Key),
            Field('gender', String(), "female", field_type=FieldType.Value),
            Field('timestamp', Int(), 0, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['action_type'] == 'gender_type'].copy()
        return filtered_df[['uid', 'gender', 'timestamp']]


class GenderLikeCountWithKVAgg(Count):
    def __init__(self, stream, windows: List[Window]):
        super().__init__('TestGenderLikeCountWithKVAgg', stream, windows)

    @classmethod
    def schema(cls) -> Schema:
        return Schema(
            Field('gender', Int(), 0, field_type=FieldType.Key),
            Field('count', Int(), 0, field_type=FieldType.Value),
            Field('timestamp', Int(), 0, field_type=FieldType.Timestamp),
        )

    @classmethod
    @depends_on(
        aggregates=[UserGenderKVAgg]
    )
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['action_type'] == 'like'].copy()
        filtered_df.reset_index(inplace=True)
        user_gender = UserGenderKVAgg.lookup(uids=filtered_df['actor_id'], window=[windows.DAY])
        filtered_df.rename(columns={'actor_id': 'uid'}, inplace=True)
        gender_df = pd.DataFrame({'gender': user_gender})
        new_df = pd.concat([filtered_df, gender_df], axis=1)
        new_df['count'] = 1
        return new_df[['gender', 'count', 'timestamp']]


def test_AggregatePreprocess_WithMockAgg(grpc_stub, mocker):
    def side_effect(*args, **kwargs):
        return pd.Series(["male", "female", "female"])

    mocker.patch(__name__ + '.aggregate_lookup', side_effect=side_effect)
    agg1 = UserGenderKVAgg('actions', [windows.DAY * 7])
    agg2 = GenderLikeCountWithKVAgg('actions', [windows.DAY * 7, windows.DAY * 28])
    workspace = WorkspaceTest(grpc_stub)
    responses = workspace.register_aggregates(agg1, agg2)

    assert len(responses) == 2
    assert responses[0].code == 200
    create_agg = CreateAggregateRequest()
    responses[0].details[0].Unpack(create_agg)
    df = pd.DataFrame(
        {
            'actor_id': [1, 2, 3, 4, 5],
            'target_id': [1, 2, 3, 4, 5],
            'timestamp': [1.1, 2.1, 3.1, 4.1, 5.1],
            'action_type': ['like', 'like', 'share', 'comment', 'like'],
        })
    processed_df = agg2.preprocess(df)
    assert processed_df.shape == (3, 3)
    assert processed_df['gender'].tolist() == ["male", "female", "female"]
    assert processed_df['timestamp'].tolist() == [1.1, 2.1, 5.1]
    assert processed_df['count'].tolist() == [1, 1, 1]


############################################################################################################
# Feature Tests
############################################################################################################

############################################################################################################
# Workspace Tests
############################################################################################################

#
# class Test:
#     def __init__(self):
#         pass
#
#     def __enter__(self):
#         pass
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         pass


"""
    Unit
        # - stream's transform function for a populator works -- no mocks
        # - preaggrgate works (df -> df) -- this needs mock for aggregates
        # - df -> aggregated data (talk to server, mock)
        - everything works in env specified
        # - feature computation works given input streams (mock of aggregates/features)
    
    Integration
        - e2e == read from stream, e2e
        - e2e == artificial stream data, e2e aggregate
        - e2e == read from stream, populate aggregates, read features
"""
