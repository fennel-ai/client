from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

# docsnip datasets
from fennel.connectors import source, Webhook
from fennel.datasets import Count, Sum, Average
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.dtypes import Continuous
from fennel.lib import includes, meta, inputs, outputs

__owner__ = "test@test.com"
webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("RatingActivity"), disorder="14d", cdc="append")
@dataset
class RatingActivity:
    userid: int
    rating: float
    movie: str
    t: datetime


@dataset(index=True)
class MovieRating:
    movie: str = field(key=True)
    rating: float
    num_ratings: int
    sum_ratings: float
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_aggregate(cls, activity: Dataset):
        return activity.groupby("movie").aggregate(
            num_ratings=Count(window=Continuous("7d")),
            sum_ratings=Sum(window=Continuous("28d"), of="rating"),
            rating=Average(window=Continuous("12h"), of="rating"),
        )


# /docsnip


# docsnip datasets_testing
import unittest
from fennel.testing import mock, log  # docsnip-highlight


class TestDataset(unittest.TestCase):
    # docsnip-highlight next-line
    @mock
    def test_dataset(self, client):  # docsnip-highlight
        # Sync the dataset
        client.commit(
            message="datasets: add RatingActivity and MovieRating",
            datasets=[MovieRating, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)

        data = [
            [18231, 2, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 2, "Jumanji", three_hours_ago],
            [18231, 5, "Jumanji", five_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 5, "Titanic", now - timedelta(minutes=1)],
            [18231, 3, "Titanic", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        log(RatingActivity, df)

        # Do some lookups to verify pipeline_aggregate
        # is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieRating.lookup(ts, movie=names)
        assert df.shape == (2, 5)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]


# /docsnip

# docsnip featuresets_testing
from fennel.featuresets import feature, featureset, extractor


@meta(owner="test@test.com")
@featureset
class UserInfoFeatures:
    userid: int
    name: str
    # The users gender among male/female/non-binary
    age: int = feature().meta(owner="aditya@fennel.ai")
    age_squared: int
    age_cubed: int
    is_name_common: bool

    @extractor
    @inputs(age, "name")
    @outputs("age_squared", "age_cubed", "is_name_common")
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        is_name_common = name.isin(["John", "Mary", "Bob"])
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            str(cls.age_squared),
            str(cls.age_cubed),
            str(cls.is_name_common),
        ]
        return df


# somewhere in the test file, you can write this
class TestSimpleExtractor(unittest.TestCase):
    def test_get_age_and_name_features(self):
        age = pd.Series([32, 24])
        name = pd.Series(["John", "Rahul"])
        ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 1)])
        df = UserInfoFeatures.get_age_and_name_features(
            UserInfoFeatures, ts, age, name
        )
        self.assertEqual(df.shape, (2, 3))
        self.assertEqual(
            df["UserInfoFeatures.age_squared"].tolist(), [1024, 576]
        )
        self.assertEqual(
            df["UserInfoFeatures.age_cubed"].tolist(), [32768, 13824]
        )
        self.assertEqual(
            df["UserInfoFeatures.is_name_common"].tolist(),
            [True, False],
        )


# /docsnip


def get_country_geoid(country: str) -> int:
    if country == "Russia":
        return 1
    elif country == "Chile":
        return 3
    else:
        return 5


# docsnip featuresets_testing_with_dataset
@meta(owner="test@test.com")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


@meta(owner="test@test.com")
@featureset
class UserInfoMultipleExtractor:
    userid: int
    name: str
    country_geoid: int
    # The users gender among male/female/non-binary
    age: int = feature().meta(owner="aditya@fennel.ai")
    age_squared: int
    age_cubed: int
    is_name_common: bool

    @extractor(deps=[UserInfoDataset])
    @inputs("userid")
    @outputs("age", "name")
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)
        return df[["age", "name"]]

    @extractor
    @inputs("age", "name")
    @outputs("age_squared", "age_cubed", "is_name_common")
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        is_name_common = name.isin(["John", "Mary", "Bob"])
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            "age_squared",
            "age_cubed",
            "is_name_common",
        ]
        return df

    @extractor(deps=[UserInfoDataset])
    @includes(get_country_geoid)
    @inputs("userid")
    @outputs("country_geoid")
    def get_country_geoid_extractor(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["country_geoid"] = df["country"].apply(get_country_geoid)
        return df["country_geoid"]


# this is your test code in some test module
class TestExtractorDAGResolution(unittest.TestCase):
    @mock
    def test_dag_resolution(self, client):
        client.commit(
            message="user: add info datasets, featuresets",
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        # For testing only.
        log(UserInfoDataset, df)

        feature_df = client.query(
            outputs=[UserInfoMultipleExtractor],
            inputs=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))


# /docsnip
