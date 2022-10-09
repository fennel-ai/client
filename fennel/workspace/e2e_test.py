import pandas as pd
import pytest

from fennel.aggregate import Count, depends_on, KeyValue
from fennel.feature import *
from fennel.lib import Field, Schema, windows
from fennel.lib.schema import Array, Bool, Double, FieldType, Int, Map, String
from fennel.stream import MySQL, populator, Stream
# noinspection PyUnresolvedReferences
from fennel.test_lib import *

"""
Goals
    Unit - Test each concept individually
    - Ability to test stream populate functionality ( no mocks needed )
    - Ability to test aggregate preprocess functionality ( KV agg mocks needed )
    - Ability to test feature computation functionality ( Agg  & feature mocks )
    - Ability to test feature functionality  with environment variables ( ^ )
    Integration - Test the entire flow; stream -> aggregate -> feature
    - e2e tests that read data from stream and produce final features
    - e2e tests for streams that connect to the stream and produce few values
    - e2e tests that take an artificial stream and produce aggregate data
    - e2e tests that take an artificial stream and produce feature data
"""

################################################################################
#                           Tests                                              #
################################################################################

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
        [Field("actor_id", dtype=Int, default=0),
         Field("target_id", dtype=Int, default=0),
         Field("action_type", dtype=String, default="love"),
         Field("gender", dtype=Bool, default=False),
         Field("timestamp", dtype=Double, default=0.0),
         Field(
             "random_array",
             dtype=Array(Array(String)),
             default=[["a", "b", "c"], ["d", "e", "f"]],
         ),
         Field(
             "metadata",
             dtype=Map(String, Array(Int)),
             default=[["a", "b", "c"], ["d", "e", "f"]],
         )]
    )

    @classmethod
    @populator(source=mysql_src, table="actions")
    def populate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "like"].copy()
        filtered_df["metadata"] = filtered_df["target_id"].apply(
            lambda x: {str(x): [x, x + 1, x + 2]}
        )
        filtered_df["random_array"] = filtered_df["target_id"].apply(
            lambda x: [[str(x), str(x + 1), str(x + 2)]]
        )
        return filtered_df[
            [
                "actor_id",
                "target_id",
                "action_type",
                "timestamp",
                "random_array",
                "metadata",
                "gender",
            ]
        ]

    @classmethod
    @populator(source=mysql_src, table="actions")
    def invalid_populate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "like"].copy()
        filtered_df.loc[:, "random_array"] = (
            filtered_df["target_id"]
            .apply(lambda x: {str(x): [x, x + 1, x + 2]})
            .copy()
        )
        filtered_df.loc[:, "metadata"] = filtered_df["target_id"].apply(
            lambda x: [[str(x), str(x + 1), str(x + 2)]]
        )
        return filtered_df[
            [
                "actor_id",
                "target_id",
                "action_type",
                "timestamp",
                "random_array",
                "metadata",
                "gender",
            ]
        ]


################################################################################
# Stream Tests
################################################################################


def test_StreamProcess():
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "action_type": ["like", "like", "share", "comment", "like"],
            "gender": [True, False, True, False, True],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
        }
    )
    processed_df = Actions.populate(df)
    assert processed_df.shape == (3, 7)


def test_InvalidPopulate_StreamProcess():
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "action_type": ["like", "like", "share", "comment", "like"],
            "gender": [True, False, True, False, True],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
        }
    )
    with pytest.raises(Exception) as e:
        Actions.invalid_populate(df)
    assert (
            str(e.value) == "Column random_array value {'1': [1, 2, 3]} failed "
                            "validation: [TypeError(\"Expected list, got <class 'dict'>\")]"
    )


################################################################################
# Aggregate Tests
################################################################################


class UserLikeCount(Count):
    name = "TestUserLikeCount"
    stream = "actions"
    windows = [windows.DAY * 1, windows.DAY * 7, windows.DAY * 14]
    schema = Schema(
        [Field("actor_id", Int, 0, field_type=FieldType.Key),
         Field("target_id", Int, 0, field_type=FieldType.Value),
         Field("timestamp", Double, 0.0, field_type=FieldType.Timestamp)]
    )

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "like"].copy()
        filtered_df["actor_id"].rename("uid")
        filtered_df.drop(columns=["action_type"], inplace=True)
        return filtered_df


def test_AggregatePreprocess(create_test_workspace):
    workspace = create_test_workspace({})
    workspace.register_aggregates(UserLikeCount)

    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    processed_df = UserLikeCount.preaggregate(df)
    assert processed_df.shape == (3, 3)
    assert processed_df["actor_id"].tolist() == [1, 2, 5]
    assert processed_df["timestamp"].tolist() == [1.1, 2.1, 5.1]
    assert processed_df["target_id"].tolist() == [1, 2, 5]


class UserLikeCountInvalidSchema(Count):
    name = "TestUserLikeCount"
    stream = "actions"
    windows = [windows.DAY * 7, windows.DAY * 28]
    schema = Schema(
        [Field("actor_id", Int, 0, field_type=FieldType.Key),
         Field("target_id", Int, 0, field_type=FieldType.Value),
         Field("timestamp", Double, "123", field_type=FieldType.Timestamp),
         ]
    )

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "like"].copy()
        filtered_df["actor_id"].rename("uid")
        filtered_df.drop(columns=["action_type"], inplace=True)
        filtered_df["timestamp"] = filtered_df["timestamp"].apply(
            lambda x: str(x)
        )
        return filtered_df


def test_AggregatePreprocessInvalidSchema(create_test_workspace):
    workspace = create_test_workspace({})
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    with pytest.raises(Exception) as e:
        workspace.register_aggregates(UserLikeCountInvalidSchema)
        _ = UserLikeCountInvalidSchema.preaggregate(df)
    assert (
            str(e.value)
            == """[TypeError('Expected default value for field timestamp to be float, got 123')]"""
    )


class UserGenderKVAgg(KeyValue):
    name = "TestUserGenderKVAgg"
    stream = "actions"
    windows = [windows.DAY * 7, windows.DAY * 28]

    schema = Schema(
        [Field("uid", dtype=Int, default=0, field_type=FieldType.Key),
         Field("gender", dtype=String, default="female",
             field_type=FieldType.Value),
         Field("timestamp", dtype=Double, default=0.0,
             field_type=FieldType.Timestamp),
         ])

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "gender_type"].copy()
        return filtered_df[["uid", "gender", "timestamp"]]


class GenderLikeCountWithKVAgg(Count):
    name = "TestGenderLikeCountWithKVAgg"
    stream = "actions"
    windows = [windows.DAY * 7, windows.DAY * 28]
    schema = Schema([Field("gender", String, "male", field_type=FieldType.Key),
                     Field("count", Int, 0, field_type=FieldType.Value),
                     Field("timestamp", Double, 0.0,
                         field_type=FieldType.Timestamp)])

    @classmethod
    @depends_on(aggregates=[UserGenderKVAgg])
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        filtered_df = df[df["action_type"] == "like"].copy()
        filtered_df.reset_index(inplace=True)
        user_gender = UserGenderKVAgg.lookup(
            uids=filtered_df["actor_id"], window=[windows.DAY]
        )
        filtered_df.rename(columns={"actor_id": "uid"}, inplace=True)
        gender_df = pd.DataFrame({"gender": user_gender})
        new_df = pd.concat([filtered_df, gender_df], axis=1)
        new_df["count"] = 1
        return new_df[["gender", "count", "timestamp"]]


def test_client_AggregatePreprocess(create_test_workspace):
    workspace = create_test_workspace(
        {UserGenderKVAgg: pd.Series(["male", "female", "male"])}
    )
    print(type(UserGenderKVAgg()))
    workspace.register_aggregates(UserGenderKVAgg, GenderLikeCountWithKVAgg)
    df = pd.DataFrame(
        {
            "actor_id": [1, 2, 3, 4, 5],
            "target_id": [1, 2, 3, 4, 5],
            "timestamp": [1.1, 2.1, 3.1, 4.1, 5.1],
            "action_type": ["like", "like", "share", "comment", "like"],
        }
    )
    processed_df = GenderLikeCountWithKVAgg.preaggregate(df)
    assert processed_df.shape == (3, 3)
    assert processed_df["gender"].tolist() == ["male", "female", "male"]
    assert processed_df["timestamp"].tolist() == [1.1, 2.1, 5.1]
    assert processed_df["count"].tolist() == [1, 1, 1]


################################################################################
# Feature Tests
################################################################################


@feature(
    name="user_like_count",
    schema=Schema([Field("user_like_count_7days", Int, 0)]),
)
@depends_on(aggregates=[UserLikeCount], )
def user_like_count_3days(uids: pd.Series) -> pd.Series:
    day7, day28 = UserLikeCount.lookup(
        uids=uids, window=[windows.DAY, windows.WEEK]
    )
    day7 = day7.apply(lambda x: x * x)
    return day7


def test_Feature(create_test_workspace):
    workspace = create_test_workspace(
        {UserLikeCount: (pd.Series([6, 12, 13]), pd.Series([5, 12, 13]))}
    )
    workspace.register_aggregates(UserLikeCount)
    workspace.register_features(user_like_count_3days)
    features = user_like_count_3days.extract(uids=pd.Series([1, 2, 3, 4, 5]))
    assert type(features) == pd.Series
    assert features[0] == 36


@feature(
    name="user_like_count_7days_random_sq",
    schema=Schema(
        [Field("user_like_count_7days_random_sq", Int, 0)],
    ),
)
@depends_on(
    aggregates=[UserLikeCount],
    features=[user_like_count_3days],
)
def user_like_count_3days_square_random(uids: pd.Series) -> pd.Series:
    user_count_features = user_like_count_3days.extract(uids=uids)
    day7, day28 = UserLikeCount.lookup(
        uids=uids, window=[windows.DAY, windows.WEEK]
    )
    day7_sq = day7.apply(lambda x: x * x)
    user_count_features_sq = user_count_features.apply(lambda x: x * x)
    return day7_sq + user_count_features_sq


def test_Feature_Agg_And_FeatureMock2(create_test_workspace):
    workspace = create_test_workspace({
        UserLikeCount: (pd.Series([6, 12, 13]), pd.Series([5, 12, 13])),
        user_like_count_3days.name: pd.Series([36, 144, 169]),
    })
    workspace.register_aggregates(UserLikeCount)
    workspace.register_features(
        user_like_count_3days, user_like_count_3days_square_random
    )
    features = user_like_count_3days_square_random.extract(
        uids=pd.Series([1, 2, 3, 4, 5])
    )
    assert type(features) == pd.Series
    # 36 * 36 + 6 * 6 = 1296 + 36 = 1332
    # 144 * 144 + 12 * 12 = 20736 + 144 = 20880
    # 169 * 169 + 13 * 13 = 28561 + 169 = 28730
    assert features.tolist() == [1332, 20880, 28730]

################################################################################
# Workspace Tests
################################################################################
