from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

# docsnip datasets
from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Dataset,
    Count,
    Sum,
    Average,
)
from fennel.lib import includes, meta, inputs, outputs
from fennel.sources import source, Webhook

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("RatingActivity"), disorder="14d", cdc="append")
@dataset
class RatingActivity:
    userid: int
    rating: float
    movie: str
    t: datetime


@meta(owner="test@test.com")
@dataset
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
            Count(window="7d", into_field="num_ratings"),
            Sum(window="28d", of="rating", into_field="sum_ratings"),
            Average(window="12h", of="rating", into_field="rating"),
        )


# /docsnip


# docsnip datasets_testing
import unittest

from fennel.testing import mock


class TestDataset(unittest.TestCase):
    @mock
    def test_dataset(self, client):
        # Sync the dataset
        client.commit(
            message="datasets: add RatingActivity and MovieRating",
            datasets=[MovieRating, RatingActivity],
        )
        now = datetime.now()
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
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK

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
    userid: int = feature(id=1)
    name: str = feature(id=2)
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    @inputs(age, name)
    @outputs(age_squared, age_cubed, is_name_common)
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
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="append")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


@meta(owner="test@test.com")
@featureset
class UserInfoMultipleExtractor:
    userid: int = feature(id=1)
    name: str = feature(id=2)
    country_geoid: int = feature(id=3)
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor(depends_on=[UserInfoDataset])
    @inputs(userid)
    @outputs(age, name)
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)
        return df[["age", "name"]]

    @extractor
    @inputs(age, name)
    @outputs(age_squared, age_cubed, is_name_common)
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

    @extractor(depends_on=[UserInfoDataset])
    @includes(get_country_geoid)
    @inputs(userid)
    @outputs(country_geoid)
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
        now = datetime.utcnow()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        feature_df = client.query(
            outputs=[UserInfoMultipleExtractor],
            inputs=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))


# /docsnip
