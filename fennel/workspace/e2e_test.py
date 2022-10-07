from typing import List

import pandas as pd
import pytest

from fennel.aggregate import (Count, depends_on, KeyValue)
# noinspection PyUnresolvedReferences
from fennel.feature import aggregate_lookup, feature, feature_extract
from fennel.lib import (Field, Schema, windows)
from fennel.lib.schema import (Array, Bool, Double, FieldType, Int, Map, String)
from fennel.lib.windows import Window
from fennel.stream import (MySQL, source, Stream)
from fennel.test_lib import *

"""
Goals
    Unit - Test each concept individually
    - Ability to test stream populate functionality ( no mocks needed )
    - Ability to test aggregate preprocess functionality ( KV agg mocks needed )
    - Ability to test feature computation functionality ( Agg  & feature mocks needed )
    - Ability to test feature functionality  with environment variables ( Agg & feature mocks needed )
    
    Integration - Test the entire flow; stream -> aggregate -> feature ( or subset of it )
    - e2e tests that read data from stream and produce final features ( talk to server )
    - e2e tests for streams that connect to the stream and produce few stream values ( talk to server )
    - e2e tests that take an artificial stream and produce aggregate data ( mocks )
    - e2e tests that take an artificial stream and produce feature data ( mocks )
"""


# create_test_workspace fixture allows you to mock aggregates and features
# You need to provide a dictionary of aggregate/feature and the return value.
# Future work: Add support for providing ability to create mocks depending on the input.
@pytest.fixture
def create_test_workspace(grpc_stub, mocker):
    def workspace(mocks):
        def agg_side_effect(agg_name, *args, **kwargs):
            for k, v in mocks.items():
                if type(k) == str:
                    continue
                if agg_name == k.instance().name:
                    return v
            raise Exception(f'Mock for {agg_name} not found')

        mocker.patch(__name__ + '.aggregate_lookup', side_effect=agg_side_effect)

        def feature_side_effect(feature_name, *args, **kwargs):
            for k, v in mocks.items():
                if type(k) != str:
                    continue
                if feature_name == k:
                    return v
            raise Exception(f'Mock for {feature_name} not found')

        mocker.patch(__name__ + '.feature_extract', side_effect=feature_side_effect)
        return ClientTestWorkspace(grpc_stub, mocker)

    return workspace


############################################################################################################
#                                                       Tests                                              #
############################################################################################################

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
            Field('timestamp', Double(), 0.0, field_type=FieldType.Timestamp),
        )

    @classmethod
    def preprocess(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df['action_type'] == 'like'].copy()
        filtered_df['actor_id'].rename('uid')
        filtered_df.drop(columns=['action_type'], inplace=True)
        return filtered_df


def test_AggregatePreprocess(create_test_workspace):
    workspace = create_test_workspace({})
    agg = UserLikeCount('actions', [windows.DAY * 7, windows.DAY * 28])
    workspace.register_aggregates(agg)

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
            Field('timestamp', Double(), 0.0, field_type=FieldType.Timestamp),
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
            Field('gender', String(), "male", field_type=FieldType.Key),
            Field('count', Int(), 0, field_type=FieldType.Value),
            Field('timestamp', Double(), 0.0, field_type=FieldType.Timestamp),
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


def test_client_AggregatePreprocess(create_test_workspace):
    workspace = create_test_workspace({UserGenderKVAgg: pd.Series(["male", "female", "male"])})
    agg1 = UserGenderKVAgg('actions', [windows.DAY * 7])
    agg2 = GenderLikeCountWithKVAgg('actions', [windows.DAY * 7, windows.DAY * 28])
    workspace.register_aggregates(agg1, agg2)
    df = pd.DataFrame(
        {
            'actor_id': [1, 2, 3, 4, 5],
            'target_id': [1, 2, 3, 4, 5],
            'timestamp': [1.1, 2.1, 3.1, 4.1, 5.1],
            'action_type': ['like', 'like', 'share', 'comment', 'like'],
        })
    processed_df = agg2.preprocess(df)
    assert processed_df.shape == (3, 3)
    assert processed_df['gender'].tolist() == ["male", "female", "male"]
    assert processed_df['timestamp'].tolist() == [1.1, 2.1, 5.1]
    assert processed_df['count'].tolist() == [1, 1, 1]


############################################################################################################
# Feature Tests
############################################################################################################


@feature(
    name='user_like_count',
    schema=Schema(
        Field('user_like_count_7days', Int(), 0),
    ),
)
@depends_on(
    aggregates=[UserLikeCount],
)
def user_like_count_3days(uids: pd.Series) -> pd.Series:
    day7, day28 = UserLikeCount.lookup(uids=uids, window=[windows.DAY, windows.WEEK])
    day7 = day7.apply(lambda x: x * x)
    return day7


def test_Feature(create_test_workspace):
    workspace = create_test_workspace({UserLikeCount: (pd.Series([6, 12, 13]),
                                                       pd.Series([5, 12, 13]))})
    workspace.register_aggregates(UserLikeCount('actions', [windows.DAY * 7, windows.DAY * 28]))
    workspace.register_features(user_like_count_3days)
    features = user_like_count_3days.extract(uids=pd.Series([1, 2, 3, 4, 5]))
    assert type(features) == pd.Series
    assert features[0] == 36


@feature(
    name='user_like_count_7days_random_sq',
    schema=Schema(
        Field('user_like_count_7days_random_sq', Int(), 0),
    ),
)
@depends_on(
    aggregates=[UserLikeCount],
    features=[user_like_count_3days],
)
def user_like_count_3days_square_random(uids: pd.Series) -> pd.Series:
    user_count_features = user_like_count_3days.extract(uids=uids)
    day7, day28 = UserLikeCount.lookup(uids=uids, window=[windows.DAY, windows.WEEK])
    day7_sq = day7.apply(lambda x: x * x)
    user_count_features_sq = user_count_features.apply(lambda x: x * x)
    return day7_sq + user_count_features_sq


def test_Feature_Agg_And_FeatureMock2(create_test_workspace):
    workspace = create_test_workspace({UserLikeCount: (pd.Series([6, 12, 13]),
                                                       pd.Series([5, 12, 13])),
                                       user_like_count_3days.name: pd.Series([36, 144, 169])
                                       })
    workspace.register_aggregates(UserLikeCount('actions', [windows.DAY * 7, windows.DAY * 28]))
    workspace.register_features(user_like_count_3days, user_like_count_3days_square_random)
    features = user_like_count_3days_square_random.extract(uids=pd.Series([1, 2, 3, 4, 5]))
    assert type(features) == pd.Series
    # 36 * 36 + 6 * 6 = 1296 + 36 = 1332
    assert features[0] == 1332

############################################################################################################
# Workspace Tests
############################################################################################################
