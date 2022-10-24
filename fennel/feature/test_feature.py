import pickle

import pandas as pd
import pytest

import fennel.gen.feature_pb2 as feature_proto
from fennel.aggregate import Aggregate, Count, depends_on
# noinspection PyUnresolvedReferences
from fennel.feature import family, single
from fennel.lib import Field, Schema, windows
from fennel.lib.schema import Int, Now, Timestamp
from fennel.test_lib import *
from fennel.utils import square


class UserLikeCount(Aggregate):
    name = "TestUserLikeCount"
    stream = "Actions"
    schema = Schema(
        [
            Field("uid", dtype=Int, default=0),
            Field(
                "count",
                dtype=Int,
                default=0,
            ),
            Field("timestamp", dtype=Timestamp, default=Now),
        ],
    )
    aggregation = Count(
        key="actor_id",
        value="target_id",
        timestamp="timestamp",
        windows=[windows.DAY * 7],
    )

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df["action_type"] == "like"]
        df["actor_id"].rename("uid")
        df = df.drop("action_type")
        return df


@single(
    name="user_like_count",
    schema=Schema([Field("user_like_count_7days", Int, 0)]),
)
@depends_on(
    aggregates=[UserLikeCount],
)
def user_like_count_3days(uids: pd.Series) -> pd.Series:
    day7, day28 = UserLikeCount.lookup(
        uids=uids, window=[windows.DAY, windows.WEEK]
    )
    day7 = day7.apply(lambda x: square(x))
    return day7


def test_FeatureRegistration2(grpc_stub, mocker):
    mocker.patch(
        "fennel.aggregate.aggregate.aggregate_lookup",
        return_value=(pd.Series([6, 12, 13]), pd.Series([5, 12, 13])),
    )
    workspace = InternalTestWorkspace(grpc_stub)
    workspace.register_aggregates(UserLikeCount)
    responses = workspace.register_features(user_like_count_3days)
    assert len(responses) == 1
    create_feature = feature_proto.CreateFeatureRequest()
    responses[0].details[0].Unpack(create_feature)
    feature_func = pickle.loads(create_feature.function)
    features = feature_func(uids=pd.Series([1, 2, 3, 4, 5]))
    assert type(features) == pd.Series
    assert features[0] == 36


@family(
    name="user_like_count",
    schema=Schema(
        [
            Field("user_like_count_7days", dtype=Int, default=0),
            Field("user_like_count_7days_sqrt", dtype=Int, default=0),
            Field("user_like_count_7days_sq", dtype=Int, default=1),
            Field("user_like_count_28days", dtype=Int, default=1),
            Field("user_like_count_28days_sqrt", dtype=Int, default=2),
            Field("user_like_count_28days_sq", dtype=Int, default=2),
        ],
    ),
)
@depends_on(
    aggregates=[UserLikeCount],
)
def user_like_count_3days_pack(uids: pd.Series) -> pd.DataFrame:
    day7, day28 = UserLikeCount.lookup(
        uids=uids, window=[windows.DAY, windows.WEEK]
    )
    day7_sq = day7 ** 2
    day7_sqrt = day7 ** 0.5
    day28_sq = day28 ** 2
    day28_sqrt = day28 ** 0.5
    return pd.DataFrame(
        {
            "user_like_count_1day": day7,
            "user_like_count_7days": day28,
            "user_like_count_7days_sqrt": day7_sqrt,
            "user_like_count_7days_sq": day7_sq,
            "user_like_count_28days": day28,
            "user_like_count_28days_sqrt": day28_sqrt,
            "user_like_count_28days_sq": day28_sq,
        }
    )


def test_FeaturePackRegistration(grpc_stub, mocker):
    mocker.patch(
        "fennel.aggregate.aggregate.aggregate_lookup",
        return_value=(
            pd.Series([6, 12, 13, 15, 156]),
            pd.Series([5, 12, 13, 34, 156]),
        ),
    )
    workspace = InternalTestWorkspace(grpc_stub)
    workspace.register_aggregates(UserLikeCount)
    responses = workspace.register_features(user_like_count_3days_pack)
    assert len(responses) == 1
    create_feature = feature_proto.CreateFeatureRequest()
    responses[0].details[0].Unpack(create_feature)
    feature_func = pickle.loads(create_feature.function)
    features = feature_func(uids=pd.Series([1, 2, 3, 4, 5]))
    assert type(features) == pd.DataFrame
    assert features.shape == (5, 7)
    assert features["user_like_count_1day"][0] == 6


@family(
    name="user_like_count",
    schema=Schema(
        [
            Field("user_like_count_7days", Int, 0),
            Field("user_like_count_7days_sqrt", Int, 0),
            Field("user_like_count_7days_sq", Int, 1),
            Field("user_like_count_28days", Int, 1),
            Field("user_like_count_28days_sqrt", Int, 2),
            Field("user_like_count_28days_sq", Int, 2),
        ],
    ),
)
@depends_on(
    aggregates=[UserLikeCount],
)
def user_like_count_3days_pack_invalid(uids: pd.Series) -> pd.Series:
    day7, day28 = UserLikeCount.lookup(
        uids=uids, window=[windows.DAY, windows.WEEK]
    )
    return day7


def test_FeaturePackRegistrationInvalid(grpc_stub, mocker):
    mocker.patch(
        "fennel.aggregate.aggregate.aggregate_lookup",
        return_value=(
            pd.Series([6, 12, 13, 15, 156]),
            pd.Series([5, 12, 13, 34, 156]),
        ),
    )
    with pytest.raises(Exception) as e:
        workspace = InternalTestWorkspace(grpc_stub)
        workspace.register_aggregates(UserLikeCount)
        workspace.register_features(user_like_count_3days_pack_invalid)
    assert str(e.value) == "['feature function must return a pandas.Series']"


@single(
    name="user_like_count",
    schema=Schema(
        [Field("user_like_count_7days", Int, 0)],
    ),
)
def user_like_count_3days_invalid_dependency(uids: pd.Series) -> pd.Series:
    day7, day28 = UserLikeCount.lookup(
        uids=uids, window=[windows.DAY, windows.WEEK]
    )
    day7 = day7.apply(lambda x: x * x)
    return day7

# AST checks are disabled for now
# def test_FeatureRegistrationInvalidDependency(grpc_stub, mocker):
#     mocker.patch(
#         __name__ + ".aggregate_lookup",
#         return_value=(pd.Series([6, 12, 13]), pd.Series([5, 12, 13])),
#     )
#     with pytest.raises(Exception) as e:
#         workspace = InternalTestWorkspace(grpc_stub)
#         workspace.register_aggregates(UserLikeCount)
#         workspace.register_features(user_like_count_3days_invalid_dependency)
#     assert (
#             str(e.value)
#             == "aggregate UserLikeCount not included in feature definition"
#     )
