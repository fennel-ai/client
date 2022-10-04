from typing import List

import pandas as pd
import ast

from fennel.aggregate import (Count, aggregate_lookup)
from fennel.feature import feature, feature_pack
from fennel.lib import (Field, Schema, windows)
from fennel.lib.schema import (FieldType, Int)
from fennel.lib.windows import Window
from fennel.test_lib import *


class UserLikeCount(Count):
    def __init__(self, stream, windows: List[Window]):
        self.name = 'TestUserLikeCount'
        self.stream = stream
        self.windows = windows

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


@feature(
    name='user_like_count',
    depends_on_aggregates=[UserLikeCount],
    schema=Schema(
        Field('user_like_count_7days', Int(), 0),
    ),
)
def user_like_count_3days(uids: pd.Series) -> pd.Series:
    day7, day28 = UserLikeCount.lookup(uids=uids, window=[windows.Day, windows.Week])
    # day7, day28 = aggregate_lookup('TestUserLikeCount', uids=uids, window=[windows.Day, windows.Week])
    # agg_name = UserLikeCount.name
    # return agg_name
    return day7


def test_FeatureRegistration(grpc_stub):
    workspace = WorkspaceTest(grpc_stub)
    responses = workspace.register_aggregates(UserLikeCount('actions', [windows.DAY * 7, windows.DAY * 28]))
    cde = user_like_count_3days.__code__
    print(cde)
    responses = workspace.register_features(user_like_count_3days)
    assert len(responses) == 1

# @feature_pack(
#     name='user_like_count',
#     depends_on_aggregates=[UserLikeCount],
#     schema=Schema(
#         Field('user_like_count_7days', Int(), 0),
#         Field('user_like_count_7days_sqrt', Int(), 0),
#         Field('user_like_count_7days_sq', Int(), 1),
#         Field('user_like_count_28days', Int(), 1),
#         Field('user_like_count_28days_sqrt', Int(), 2),
#         Field('user_like_count_28days_sq', Int(), 2),
#     ),
# )
# def user_like_count_3days(uids: pd.Series) -> pd.DataFrame:
#     day7, day28 = UserLikeCount.Lookup(uids=uids, window=[windows.Day, windows.Week])
#     day7_sq = day7 ** 2
#     day7_sqrt = day7 ** 0.5
#     day28_sq = day28 ** 2
#     day28_sqrt = day28 ** 0.5
#     return pd.DataFrame(
#         {'user_like_count_1day': day7, 'user_like_count_7days': day28, 'user_like_count_7days_sqrt': day7_sqrt,
#          'user_like_count_7days_sq': day7_sq, 'user_like_count_28days': day28,
#          'user_like_count_28days_sqrt': day28_sqrt, 'user_like_count_28days_sq': day28_sq})
#
#
# def test_FeaturePackRegistration(grpc_stub):
#     assert 1 == 1
