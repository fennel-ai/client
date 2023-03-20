from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests

from fennel.lib.schema import Series, DataFrame

# docsnip datasets
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Count, Sum, Average
from fennel.lib.window import Window
from fennel.lib.metadata import meta


@meta(owner="test@test.com")
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

    @classmethod
    @pipeline(id=1)
    def pipeline_aggregate(cls, activity: Dataset[RatingActivity]):
        return activity.groupby("movie").aggregate(
            [
                Count(window=Window("7d"), into_field="num_ratings"),
                Sum(
                    window=Window("28d"), of="rating", into_field="sum_ratings"
                ),
                Average(window=Window("12h"), of="rating", into_field="rating"),
            ]
        )
# /docsnip

# docsnip datasets_testing
import unittest

from fennel.test_lib import mock_client


class TestDataset(unittest.TestCase):
    @mock_client
    def test_dataset(self, client):
        # Sync the dataset
        client.sync(
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
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify pipeline_aggregate
        # is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieRating.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (2, 5)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]
# /docsnip

# docsnip featuresets_testing
from fennel.featuresets import feature, featureset, extractor, depends_on


@meta(owner="test@test.com")
@featureset
class UserInfoMultipleExtractor:
    userid: int = feature(id=1)
    name: str = feature(id=2)
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    def get_age_and_name_features(
        cls, ts: Series[datetime], user_age: Series[age], name: Series[name]
    ) -> DataFrame[age_squared, age_cubed, is_name_common]:
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
        df = UserInfoMultipleExtractor.get_age_and_name_features(
            UserInfoMultipleExtractor, ts, age, name
        )
        self.assertEqual(df.shape, (2, 3))
        self.assertEqual(
            df["UserInfoMultipleExtractor.age_squared"].tolist(), [1024, 576]
        )
        self.assertEqual(
            df["UserInfoMultipleExtractor.age_cubed"].tolist(), [32768, 13824]
        )
        self.assertEqual(
            df["UserInfoMultipleExtractor.is_name_common"].tolist(),
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

    @extractor
    @depends_on(UserInfoDataset)
    def get_user_age_and_name(
        cls, ts: Series[datetime], user_id: Series[userid]
    ) -> DataFrame[age, name]:
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)
        return df[["age", "name"]]

    @extractor
    def get_age_and_name_features(
        cls, ts: Series[datetime], user_age: Series[age], name: Series[name]
    ) -> DataFrame[age_squared, age_cubed, is_name_common]:
        is_name_common = name.isin(["John", "Mary", "Bob"])
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            str(cls.age_squared),
            str(cls.age_cubed),
            str(cls.is_name_common),
        ]
        return df

    @extractor
    @depends_on(UserInfoDataset)
    def get_country_geoid(
        cls, ts: Series[datetime], user_id: Series[userid]
    ) -> Series[country_geoid]:
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df["country"].apply(get_country_geoid)


# this is your test code in some test module
class TestExtractorDAGResolution(unittest.TestCase):
    @mock_client
    def test_dag_resolution(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.now()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoMultipleExtractor,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))
# /docsnip
