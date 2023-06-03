import json
import re
import time
import unittest
from datetime import datetime, timedelta

import pandas as pd
import pytest
from typing import Optional, List, Dict

import fennel._vendor.requests as requests
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Min, Max
from fennel.lib.aggregate import Sum, Average, Count
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import oneof, inputs
from fennel.lib.window import Window
from fennel.sources import source, Webhook
from fennel.test_lib import mock, InternalTestClient

################################################################################
#                           Dataset Unit Tests
################################################################################

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


@meta(owner="test@test.com")
@dataset
class UserInfoDatasetDerived:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    country_name: Optional[str]
    ts: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(UserInfoDataset)
    def get_info(cls, info: Dataset):
        x = info.rename({"country": "country_name", "timestamp": "ts"})
        return x.drop(columns=["age"])


class TestDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_sync_dataset(self, client):
        """Sync the dataset and check if it is synced correctly."""
        # Sync the dataset
        response = client.sync(datasets=[UserInfoDataset])
        assert response.status_code == requests.codes.OK, response.json()

    @pytest.mark.integration
    @mock
    def test_simple_log(self, client):
        # Sync the dataset
        client.sync(datasets=[UserInfoDataset])
        now = datetime.now()
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

    @pytest.mark.integration
    @mock
    def test_simple_drop_rename(self, client):
        # Sync the dataset
        client.sync(datasets=[UserInfoDataset, UserInfoDatasetDerived])
        now = datetime.now()
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do lookup on UserInfoDataset
        if client.is_integration_client():
            client.sleep()
        ts = pd.Series([now, now])
        user_id_keys = pd.Series([18232, 18234])
        df, found = UserInfoDataset.lookup(ts, user_id=user_id_keys)
        assert df.shape == (2, 5)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["age"].tolist() == [32, 24]
        assert df["country"].tolist() == ["USA", "Chile"]

        # Do lookup on UserInfoDatasetDerived
        df, found = UserInfoDatasetDerived.lookup(ts, user_id=user_id_keys)
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["country_name"].tolist() == ["USA", "Chile"]
        # Check if column ts exists
        assert "ts" in df.columns
        # Check if column age does not exist
        assert "age" not in df.columns

    @pytest.mark.integration
    @mock
    def test_log_to_dataset(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # Sync the dataset
        client.sync(datasets=[UserInfoDataset])

        now = datetime.now()
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        data = [
            [18232, "Ross", "32", "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        df = pd.DataFrame(data, columns=columns)
        if client.is_integration_client():
            response = client.log_to_dataset("UserInfoDataset", df)
        else:
            response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        if client.is_integration_client():
            assert (
                response.json()["error"]
                == """error: input parse error: expected Int, but got String("32")"""
            )
        else:
            assert (
                response.json()["error"]
                == "Schema validation failed during data insertion to "
                "`UserInfoDataset` [ValueError('Field `age` is of type int, but the column "
                "in the dataframe is of type `object`. Error found during checking schema for `UserInfoDataset`.')]"
            )
        client.sleep(10)
        # Do some lookups
        user_ids = pd.Series([18232, 18234, 1920])
        lookup_now = datetime.now() + pd.Timedelta(minutes=1)
        ts = pd.Series([lookup_now, lookup_now, lookup_now])
        df, found = UserInfoDataset.lookup(
            ts,
            user_id=user_ids,
        )
        assert found.tolist() == [True, True, False]
        assert df["name"].tolist() == ["Ross", "Monica", None]
        assert df["age"].tolist() == [32, 24, None]
        assert df["country"].tolist() == ["USA", "Chile", None]
        if not client.is_integration_client():
            assert df["timestamp"].tolist() == [now, yesterday, None]
        else:
            df["timestamp"] = df["timestamp"].apply(
                lambda x: x.replace(second=0, microsecond=0)
            )
            now_rounded = now.replace(second=0, microsecond=0)
            yday_rounded = yesterday.replace(second=0, microsecond=0)
            assert df["timestamp"].tolist()[:2] == [
                pd.Timestamp(now_rounded),
                pd.Timestamp(yday_rounded),
            ]
        # Do some lookups with a timestamp
        user_ids = pd.Series([18232, 18234])
        six_hours_ago = now - pd.Timedelta(hours=6)
        ts = pd.Series([six_hours_ago, six_hours_ago])
        df, found = UserInfoDataset.lookup(
            ts, user_id=user_ids, fields=["name", "age", "country"]
        )
        assert found.tolist() == [False, True]
        assert df["name"].tolist() == [None, "Monica"]
        assert df["age"].tolist() == [None, 24]
        assert df["country"].tolist() == [None, "Chile"]
        tomorrow = now + pd.Timedelta(days=1)
        three_days_from_now = now + pd.Timedelta(days=3)
        data = [
            [18232, "Ross", 33, "Russia", tomorrow],
            [18234, "Monica", 25, "Columbia", three_days_from_now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK

        client.sleep()

        # Do some lookups
        one_day_from_now = now + pd.Timedelta(days=1)
        three_days_from_now = now + pd.Timedelta(days=3)
        ts = pd.Series([three_days_from_now, one_day_from_now])
        df, found = UserInfoDataset.lookup(
            ts, user_id=user_ids, fields=["name", "age", "country"]
        )
        assert found.tolist() == [True, True]
        assert df.shape == (2, 3)
        assert df["age"].tolist() == [33, 24]
        assert df["country"].tolist() == ["Russia", "Chile"]
        assert df["name"].tolist() == ["Ross", "Monica"]

        ts = pd.Series([three_days_from_now, three_days_from_now])
        df, lookup = UserInfoDataset.lookup(
            ts,
            user_id=user_ids,
            fields=["user_id", "name", "age", "country"],
        )
        assert lookup.tolist() == [True, True]
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["age"].tolist() == [33, 25]
        assert df["country"].tolist() == ["Russia", "Columbia"]

    @pytest.mark.integration
    @mock
    def test_invalid_dataschema(self, client):
        """Check if invalid data raises an error."""
        client.sync(datasets=[UserInfoDataset])
        data = [
            [18232, "Ross", "32", "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        if client.is_integration_client():
            response = client.log_to_dataset("UserInfoDataset", df)
        else:
            response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        assert len(response.json()["error"]) > 0

    @pytest.mark.integration
    @mock
    def test_deleted_field(self, client):
        with self.assertRaises(Exception) as e:

            @meta(owner="test@test.com")
            @dataset
            class UserInfoDataset:
                user_id: int = field(key=True)
                name: str
                age: Optional[int] = field().meta(deleted=True)
                country: Optional[str]
                timestamp: datetime = field(timestamp=True)

            client.sync(datasets=[UserInfoDataset])

        assert (
            str(e.exception)
            == "Dataset currently does not support deleted or deprecated fields."
        )


# On demand datasets are not supported for now.

# class TestDocumentDataset(unittest.TestCase):
#     @mock_client
#     def test_log_to_document_dataset(self, client):
#         """Log some data to the dataset and check if it is logged correctly."""
#
#         @meta(owner="aditya@fennel.ai")
#         @dataset
#         class DocumentContentDataset:
#             doc_id: int = field(key=True)
#             bert_embedding: Embedding[4]
#             fast_text_embedding: Embedding[3]
#             num_words: int
#             timestamp: datetime = field(timestamp=True)
#
#             @on_demand(expires_after="3d")
#             @inputs(datetime, int)
#             def get_embedding(cls, ts: pd.Series, doc_ids: pd.Series):
#                 data = []
#                 doc_ids = doc_ids.tolist()
#                 for i in range(len(ts)):
#                     data.append(
#                         [
#                             doc_ids[i],
#                             [0.1, 0.2, 0.3, 0.4],
#                             [1.1, 1.2, 1.3],
#                             10 * i,
#                             ts[i],
#                         ]
#                     )
#                 columns = [
#                     str(cls.doc_id),
#                     str(cls.bert_embedding),
#                     str(cls.fast_text_embedding),
#                     str(cls.num_words),
#                     str(cls.timestamp),
#                 ]
#                 return pd.DataFrame(data, columns=columns), pd.Series(
#                     [True] * len(ts)
#                 )
#
#         # Sync the dataset
#         client.sync(datasets=[DocumentContentDataset])
#         now = datetime.now()
#         data = [
#             [18232, np.array([1, 2, 3, 4]), np.array([1, 2, 3]), 10, now],
#             [
#                 18234,
#                 np.array([1, 2.2, 0.213, 0.343]),
#                 np.array([0.87, 2, 3]),
#                 9,
#                 now,
#             ],
#             [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
#         ]
#         columns = [
#             "doc_id",
#             "bert_embedding",
#             "fast_text_embedding",
#             "num_words",
#             "timestamp",
#         ]
#         df = pd.DataFrame(data, columns=columns)
#         response = client.log("fennel_webhook","DocumentContentDataset", df)
#         assert response.status_code == requests.codes.OK, response.json()
#
#         # Do some lookups
#         doc_ids = pd.Series([18232, 1728, 18234, 18934, 19200, 91012])
#         ts = pd.Series([now, now, now, now, now, now])
#         df, _ = DocumentContentDataset.lookup(ts, doc_id=doc_ids)
#         assert df.shape == (6, 5)
#         assert df["num_words"].tolist() == [10.0, 9.0, 12, 0, 10.0, 20.0]


################################################################################
#                           Dataset & Pipelines Unit Tests
################################################################################


@meta(owner="test@test.com")
@source(webhook.endpoint("RatingActivity"))
@dataset
class RatingActivity:
    userid: int
    rating: float
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"])  # type: ignore # noqa
    t: datetime


@meta(owner="test@test.com")
@dataset
class MovieRatingCalculated:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating: float
    num_ratings: int
    sum_ratings: float
    min_ratings: float
    max_ratings: float
    t: datetime

    @pipeline(version=1)
    @inputs(RatingActivity)
    def pipeline_aggregate(cls, activity: Dataset):
        return activity.groupby("movie").aggregate(
            [
                Count(
                    window=Window("forever"), into_field=str(cls.num_ratings)
                ),
                Sum(
                    window=Window("forever"),
                    of="rating",
                    into_field=str(cls.sum_ratings),
                ),
                Average(
                    window=Window("forever"),
                    of="rating",
                    into_field=str(cls.rating),
                ),
                Min(
                    window=Window("forever"),
                    of="rating",
                    into_field=str(cls.min_ratings),
                    default=5.0,
                ),
                Max(
                    window=Window("forever"),
                    of="rating",
                    into_field=str(cls.max_ratings),
                    default=0.0,
                ),
            ]
        )


# Copy of above dataset but can be used as an input to another pipeline.
@meta(owner="test@test.com")
@source(webhook.endpoint("MovieRating"))
@dataset
class MovieRating:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating: float
    num_ratings: int
    sum_ratings: float
    t: datetime


@meta(owner="test@test.com")
@dataset
class MovieRatingTransformed:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating_sq: float
    rating_cube: float
    rating_into_5: float
    t: datetime

    @pipeline(version=1)
    @inputs(MovieRating)
    def pipeline_transform(cls, m: Dataset):
        def t(df: pd.DataFrame) -> pd.DataFrame:
            df["rating_sq"] = df["rating"] * df["rating"]
            df["rating_cube"] = df["rating_sq"] * df["rating"]
            df["rating_into_5"] = df["rating"] * 5
            return df[
                ["movie", "t", "rating_sq", "rating_cube", "rating_into_5"]
            ]

        return m.transform(
            t,
            schema={
                "movie": oneof(str, ["Jumanji", "Titanic", "RaOne"]),
                "t": datetime,
                "rating_sq": float,
                "rating_cube": float,
                "rating_into_5": float,
            },
        )


class TestBasicTransform(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_transform(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, MovieRatingTransformed, RatingActivity],
        )
        now = datetime.now()
        two_hours_ago = now - timedelta(hours=2)
        data = [
            ["Jumanji", 4, 343, 789, two_hours_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["movie", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        # Do some lookups to verify pipeline_transform is working as expected
        an_hour_ago = now - timedelta(hours=1)
        ts = pd.Series([an_hour_ago, an_hour_ago])
        names = pd.Series(["Jumanji", "Titanic"])
        df, found = MovieRatingTransformed.lookup(
            ts,
            movie=names,
        )

        assert found.tolist() == [True, False]
        assert df.shape == (2, 5)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, None]
        assert df["rating_cube"].tolist() == [64, None]
        assert df["rating_into_5"].tolist() == [20, None]

        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        client.sleep()
        df, _ = MovieRatingTransformed.lookup(
            ts,
            movie=names,
        )

        assert df.shape == (2, 5)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, 25]
        assert df["rating_cube"].tolist() == [64, 125]
        assert df["rating_into_5"].tolist() == [20, 25]


@meta(owner="test@test.com")
@source(webhook.endpoint("MovieRevenue"))
@dataset
class MovieRevenue:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    revenue: int
    t: datetime


@meta(owner="aditya@fennel.ai")
@dataset
class MovieStats:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating: float
    revenue_in_millions: float
    t: datetime

    @pipeline(version=1)
    @inputs(MovieRating, MovieRevenue)
    def pipeline_join(cls, rating: Dataset, revenue: Dataset):
        def to_millions(df: pd.DataFrame) -> pd.DataFrame:
            df[str(cls.revenue_in_millions)] = df["revenue"] / 1000000
            df[str(cls.revenue_in_millions)].fillna(-1, inplace=True)
            return df[
                [
                    str(cls.movie),
                    str(cls.t),
                    str(cls.revenue_in_millions),
                    str(cls.rating),
                ]
            ]

        c = rating.left_join(revenue, on=[str(cls.movie)])
        # Transform provides additional columns which will be filtered out.
        return c.transform(
            to_millions,
            schema={
                str(cls.movie): oneof(str, ["Jumanji", "Titanic", "RaOne"]),
                str(cls.rating): float,
                str(cls.t): datetime,
                str(cls.revenue_in_millions): float,
            },
        )


class TestBasicJoin(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_join(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, MovieRevenue, MovieStats, RatingActivity],
        )
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        data = [
            ["Jumanji", 4, 343, 789, one_hour_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["movie", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()

        two_hours_ago = now - timedelta(hours=2)
        data = [["Jumanji", 2000000, two_hours_ago], ["Titanic", 50000000, now]]
        columns = ["movie", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRevenue", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do some lookups to verify pipeline_join is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        client.sleep()
        df, _ = MovieStats.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (2, 4)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [4, 5]
        assert df["revenue_in_millions"].tolist() == [2, 50]

        # Do some lookup at various timestamps in the past
        ts = pd.Series([two_hours_ago, one_hour_ago, one_hour_ago, now])
        names = pd.Series(["Jumanji", "Jumanji", "Titanic", "Titanic"])
        df, _ = MovieStats.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (4, 4)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Jumanji",
            "Titanic",
            "Titanic",
        ]
        assert df["rating"].tolist() == [None, 4, None, 5]
        assert df["revenue_in_millions"].tolist() == [None, 2, None, 50]


class TestBasicAggregate(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_aggregate(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRatingCalculated, RatingActivity],
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

        client.sleep()

        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieRatingCalculated.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (2, 7)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]
        assert df["min_ratings"].tolist() == [2, 3]
        assert df["max_ratings"].tolist() == [5, 5]


@meta(owner="test@test.com")
@dataset
class MovieRatingWindowed:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    num_ratings_3d: int
    sum_ratings_7d: float
    avg_rating_6h: float
    total_ratings: int
    t: datetime

    @pipeline(version=1)
    @inputs(RatingActivity)
    def pipeline_aggregate(cls, activity: Dataset):
        return activity.groupby("movie").aggregate(
            [
                Count(window=Window("3d"), into_field=str(cls.num_ratings_3d)),
                Sum(
                    window=Window("7d"),
                    of="rating",
                    into_field=str(cls.sum_ratings_7d),
                ),
                Average(
                    window=Window("6h"),
                    of="rating",
                    into_field=str(cls.avg_rating_6h),
                ),
                Count(
                    window=Window("forever"), into_field=str(cls.total_ratings)
                ),
            ]
        )


class TestBasicWindowAggregate(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_window_aggregate(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRatingWindowed, RatingActivity],
        )
        true_now = datetime.now()
        now = true_now - timedelta(days=10) - timedelta(minutes=1)
        three_hours = now + timedelta(hours=3)
        six_hours = now + timedelta(hours=6)
        one_day = now + timedelta(days=1)
        two_days = now + timedelta(days=2)
        three_days = now + timedelta(days=3)
        six_days = now + timedelta(days=6)
        seven_days = now + timedelta(days=7)
        data = [
            [18231, 2, "Jumanji", three_hours],
            [18231, 3, "Jumanji", six_hours],
            [18231, 2, "Jumanji", one_day],
            [18231, 5, "Jumanji", three_days],
            [18231, 4, "Jumanji", six_days],
            [18231, 4, "Jumanji", seven_days],
            [18231, 4, "Titanic", one_day],
            [18231, 3, "Titanic", two_days],
            [18231, 5, "Titanic", three_days],
            [18231, 5, "Titanic", two_days],
            [18231, 3, "Titanic", six_days],
            [18231, 3, "Titanic", seven_days],
            [18921, 1, "Jumanji", true_now],
            [18921, 1, "Titanic", true_now],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        now = true_now - timedelta(days=10)
        eight_days = now + timedelta(days=8)
        five_days = now + timedelta(days=5)
        one_day = now + timedelta(days=1)
        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series(
            [eight_days, eight_days, five_days, five_days, one_day, one_day]
        )
        names = pd.Series(
            ["Jumanji", "Titanic", "Jumanji", "Titanic", "Jumanji", "Titanic"]
        )
        df, _ = MovieRatingWindowed.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (6, 6)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Titanic",
            "Jumanji",
            "Titanic",
            "Jumanji",
            "Titanic",
        ]
        assert df["num_ratings_3d"].tolist() == [2, 2, 1, 1, 3, 1]
        assert df["sum_ratings_7d"].tolist() == [13, 19, 12, 17, 7, 4]
        assert df["avg_rating_6h"].tolist() == [0.0, 0.0, 0.0, 0.0, 2.0, 4.0]
        assert df["total_ratings"].tolist() == [6, 6, 4, 4, 3, 1]


@meta(owner="test@test.com")
@dataset
class PositiveRatingActivity:
    cnt_rating: int
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    t: datetime

    @pipeline(version=1)
    @inputs(RatingActivity)
    def filter_positive_ratings(cls, rating: Dataset):
        filtered_ds = rating.filter(lambda df: df["rating"] >= 3.5)
        filter2 = filtered_ds.filter(
            lambda df: df["movie"].isin(["Jumanji", "Titanic", "RaOne"])
        )
        return filter2.groupby("movie").aggregate(
            [Count(window=Window("forever"), into_field=str(cls.cnt_rating))],
        )


class TestBasicFilter(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_filter(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[PositiveRatingActivity, RatingActivity],
        )
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 3.5, "Jumanji", three_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 4.5, "Titanic", minute_ago],
            [18231, 2, "RaOne", one_hour_ago],
            [18231, 3, "RaOne", minute_ago],
            [18231, 1, "RaOne", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series([now, now, now])
        names = pd.Series(["Jumanji", "Titanic", "RaOne"])
        df, _ = PositiveRatingActivity.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (3, 3)
        assert df["movie"].tolist() == ["Jumanji", "Titanic", "RaOne"]
        assert df["cnt_rating"].tolist() == [2, 3, None]

        ts = pd.Series([two_hours_ago, two_hours_ago, two_hours_ago])
        df, _ = PositiveRatingActivity.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (3, 3)
        assert df["movie"].tolist() == ["Jumanji", "Titanic", "RaOne"]
        assert df["cnt_rating"].tolist() == [2, 1, None]


################################################################################
#                           Dataset & Pipelines Complex Unit Tests
################################################################################


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("Activity"))
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="me@fenne.ai")
@source(webhook.endpoint("MerchantInfo"))
@dataset(history="4m")
class MerchantInfo:
    merchant_id: int = field(key=True)
    category: str
    location: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset
class FraudReportAggregatedDataset:
    category: str = field(key=True)
    timestamp: datetime

    num_categ_fraudulent_transactions: int
    num_categ_fraudulent_transactions_7d: int
    sum_categ_fraudulent_transactions_7d: float

    @pipeline(version=1)
    @inputs(Activity, MerchantInfo)
    def create_fraud_dataset(cls, activity: Dataset, merchant_info: Dataset):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df_timestamp = pd.concat([df_json, df["timestamp"]], axis=1)
            return df_timestamp

        def fillna(df: pd.DataFrame) -> pd.DataFrame:
            df["category"] = df["category"].fillna("unknown")
            df["location"] = df["location"].fillna("unknown")
            return df

        filtered_ds = activity.filter(lambda df: df["action_type"] == "report")
        ds = filtered_ds.transform(
            extract_info,
            schema={
                "transaction_amount": float,
                "merchant_id": int,
                "timestamp": datetime,
            },
        )
        ds = ds.left_join(
            merchant_info,
            on=["merchant_id"],
        )
        ds = ds.transform(
            fillna,
            schema={
                "merchant_id": int,
                "category": str,
                "location": str,
                "timestamp": datetime,
                "transaction_amount": float,
            },
        )
        return ds.groupby("category").aggregate(
            [
                Count(
                    window=Window("forever"),
                    into_field=str(cls.num_categ_fraudulent_transactions),
                ),
                Count(
                    window=Window("1w"),
                    into_field=str(cls.num_categ_fraudulent_transactions_7d),
                ),
                Sum(
                    window=Window("1w"),
                    of="transaction_amount",
                    into_field=str(cls.sum_categ_fraudulent_transactions_7d),
                ),
            ]
        )


class TestFraudReportAggregatedDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_fraud(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MerchantInfo, Activity, FraudReportAggregatedDataset]
        )
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        data = [
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322}',
                minute_ago,
            ],
            [
                13423,
                "atm_withdrawal",
                99,
                '{"location": "mumbai"}',
                minute_ago,
            ],
            [
                14325,
                "report",
                99,
                '{"transaction_amount": 99, "merchant_id": 1422}',
                minute_ago,
            ],
            [
                18347,
                "atm_withdrawal",
                209,
                '{"location": "delhi"}',
                minute_ago,
            ],
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322}',
                minute_ago,
            ],
            [
                18232,
                "report",
                149,
                '{"transaction_amount": 149, "merchant_id": 1422}',
                minute_ago,
            ],
            [
                18232,
                "report",
                999,
                '{"transaction_amount": 999, "merchant_id": 1322}',
                minute_ago,
            ],
            [
                18232,
                "report",
                199,
                '{"transaction_amount": 199, "merchant_id": 1322}',
                minute_ago,
            ],
        ]
        columns = ["user_id", "action_type", "amount", "metadata", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "Activity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        two_days_ago = now - timedelta(days=2)
        data = [
            [1322, "grocery", "mumbai", two_days_ago],
            [1422, "entertainment", "delhi", two_days_ago],
        ]

        columns = ["merchant_id", "category", "location", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MerchantInfo", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        now = datetime.now() + timedelta(minutes=1)
        ts = pd.Series([now, now])
        categories = pd.Series(["grocery", "entertainment"])
        df, _ = FraudReportAggregatedDataset.lookup(ts, category=categories)

        assert df.shape == (2, 5)
        assert df["category"].tolist() == ["grocery", "entertainment"]
        assert df["num_categ_fraudulent_transactions"].tolist() == [4, 2]
        assert df["num_categ_fraudulent_transactions_7d"].tolist() == [4, 2]
        assert df["sum_categ_fraudulent_transactions_7d"].tolist() == [
            1296,
            248,
        ]

        if client.is_integration_client():
            return

        df = client.get_dataset_df("FraudReportAggregatedDataset")
        assert df.shape == (4, 5)
        assert df["category"].tolist() == [
            "entertainment",
            "grocery",
            "entertainment",
            "grocery",
        ]
        assert df["num_categ_fraudulent_transactions"].tolist() == [2, 4, 2, 4]
        assert df["num_categ_fraudulent_transactions_7d"].tolist() == [
            2,
            4,
            0,
            0,
        ]
        assert df["sum_categ_fraudulent_transactions_7d"].tolist() == [
            248,
            1296,
            0,
            0,
        ]


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("UserAge"))
@dataset
class UserAge:
    name: str = field(key=True)
    age: int
    city: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("UserAgeNonTable"))
@dataset
class UserAgeNonTable:
    name: str
    age: int
    city: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset
class UserAgeAggregated:
    city: str = field(key=True)
    timestamp: datetime
    sum_age: int

    @pipeline(version=1, active=True)
    @inputs(UserAge)
    def create_user_age_aggregated(cls, user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )

    @pipeline(version=2)
    @inputs(UserAgeNonTable)
    def create_user_age_aggregated2(cls, user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )


class TestAggregateTableDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_table_aggregation(self, client):
        client.sync(datasets=[UserAge, UserAgeNonTable, UserAgeAggregated])
        client.sleep()
        yesterday = datetime.now() - timedelta(days=1)
        now = datetime.now()
        tomorrow = datetime.now() + timedelta(days=1)

        data = [
            ["Sonu", 24, "mumbai", yesterday],
            ["Monu", 25, "delhi", yesterday],
        ]

        columns = ["name", "age", "city", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        ts = pd.Series([now, now])
        names = pd.Series(["mumbai", "delhi"])
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        input_df["timestamp"] = tomorrow
        response = client.log("fennel_webhook", "UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df.shape == (2, 3)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]


################################################################################
#                           Dataset & Pipelines Complex E2E Tests
################################################################################


@meta(owner="gianni@fifa.com")
@source(webhook.endpoint("PlayerInfo"))
@dataset
class PlayerInfo:
    name: str = field(key=True)
    age: int
    height: int = field().meta(description="in inches")  # type: ignore
    weight: int = field().meta(description="in pounds")  # type: ignore
    club: str
    timestamp: datetime


@meta(owner="gianni@fifa.com")
@source(webhook.endpoint("ClubSalary"))
@dataset
class ClubSalary:
    club: str = field(key=True)
    timestamp: datetime
    salary: int


@meta(owner="gianni@fifa.com")
@source(webhook.endpoint("WAG"))
@dataset
class WAG:
    name: str = field(key=True)
    timestamp: datetime
    wag: str


@meta(owner="gianni@fifa.com")
@dataset
class ManchesterUnitedPlayerInfo:
    name: str = field(key=True)
    timestamp: datetime
    age: int
    height: float = field().meta(description="in cm")  # type: ignore
    weight: float = field().meta(description="in kg")  # type: ignore
    club: str
    salary: Optional[int]
    wag: Optional[str]

    @pipeline()
    @inputs(PlayerInfo, ClubSalary, WAG)
    def create_player_detailed_info(
        cls, player_info: Dataset, club_salary: Dataset, wag: Dataset
    ):
        def convert_to_metric_stats(df: pd.DataFrame) -> pd.DataFrame:
            df["height"] = df["height"] * 2.54
            df["weight"] = df["weight"] * 0.453592
            return df

        metric_stats = player_info.transform(
            convert_to_metric_stats,
            schema={
                "name": str,
                "age": int,
                "height": float,
                "weight": float,
                "club": str,
                "timestamp": datetime,
            },
        )
        ms = metric_stats.left_join(club_salary, on=["club"])
        man_players = ms.filter(lambda df: df["club"] == "Manchester United")
        return man_players.left_join(wag, on=["name"])


@meta(owner="gianni@fifa.com")
@dataset
class ManchesterUnitedPlayerInfoBounded:
    name: str = field(key=True)
    timestamp: datetime
    age: int
    height: float = field().meta(description="in cm")  # type: ignore
    weight: float = field().meta(description="in kg")  # type: ignore
    club: str
    salary: Optional[int]
    wag: Optional[str]

    @pipeline(version=1)
    @inputs(PlayerInfo, ClubSalary, WAG)
    def create_player_detailed_info(
        cls, player_info: Dataset, club_salary: Dataset, wag: Dataset
    ):
        def convert_to_metric_stats(df: pd.DataFrame) -> pd.DataFrame:
            df["height"] = df["height"] * 2.54
            df["weight"] = df["weight"] * 0.453592
            return df

        metric_stats = player_info.transform(
            convert_to_metric_stats,
            schema={
                "name": str,
                "age": int,
                "height": float,
                "weight": float,
                "club": str,
                "timestamp": datetime,
            },
        )
        player_info_with_salary = metric_stats.left_join(
            club_salary, on=["club"], within=("60s", "0s")
        )
        manchester_players = player_info_with_salary.filter(
            lambda df: df["club"] == "Manchester United"
        )
        return manchester_players.left_join(
            wag, on=["name"], within=("forever", "60s")
        )


class TestE2eIntegrationTestMUInfo(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_muinfo_e2e_test(self, client):
        client.sync(
            datasets=[PlayerInfo, ClubSalary, WAG, ManchesterUnitedPlayerInfo],
        )
        now = datetime.now()
        yesterday = datetime.now() - timedelta(days=1)
        minute_ago = datetime.now() - timedelta(minutes=1)
        data = [
            ["Rashford", 25, 71, 154, "Manchester United", minute_ago],
            ["Maguire", 29, 76, 198, "Manchester United", minute_ago],
            ["Messi", 35, 67, 148, "PSG", minute_ago],
            ["Christiano Ronaldo", 37, 74, 187, "Al-Nassr", minute_ago],
            ["Christiano Ronaldo", 30, 74, 177, "Manchester United", yesterday],
            ["Antony", 22, 69, 139, "Manchester United", minute_ago],
        ]
        columns = ["name", "age", "height", "weight", "club", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "PlayerInfo", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            ["Manchester United", yesterday, 1000000],
            ["PSG", yesterday, 2000000],
            ["Al-Nassr", yesterday, 3000000],
        ]
        columns = ["club", "timestamp", "salary"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "ClubSalary", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            ["Rashford", yesterday, "Lucia"],
            ["Maguire", yesterday, "Fern"],
            ["Messi", yesterday, "Antonela"],
            ["Christiano Ronaldo", yesterday, "Georgina"],
            ["Antony", yesterday, "Rosilene"],
        ]
        columns = ["name", "timestamp", "wag"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "WAG", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        # Do a lookup
        # Check with Nikhil on timestamp for CR7 - yesterday
        ts = pd.Series([now, now, now, now, now])
        names = pd.Series(
            [
                "Rashford",
                "Maguire",
                "Messi",
                "Christiano Ronaldo",
                "Antony",
            ]
        )
        client.sleep()
        df, _ = ManchesterUnitedPlayerInfo.lookup(ts, name=names)
        assert df.shape == (5, 8)
        assert df["club"].tolist() == [
            "Manchester United",
            "Manchester United",
            None,
            "Manchester United",
            "Manchester United",
        ]
        assert df["salary"].tolist() == [
            1000000,
            1000000,
            None,
            1000000,
            1000000,
        ]
        assert df["wag"].tolist() == [
            "Lucia",
            "Fern",
            None,
            "Georgina",
            "Rosilene",
        ]


class TestE2eIntegrationTestMUInfoBounded(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_muinfo_e2e_bounded(self, client):
        client.sync(
            datasets=[
                PlayerInfo,
                ClubSalary,
                WAG,
                ManchesterUnitedPlayerInfoBounded,
            ],
        )

        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
        second_ahead = now + timedelta(seconds=1)
        minute_ahead = now + timedelta(minutes=1)
        couple_minutes_ahead = now + timedelta(minutes=2)
        data = [
            ["Rashford", 25, 71, 154, "Manchester United", now],
            ["Maguire", 29, 76, 198, "Manchester United", now],
            ["Messi", 35, 67, 148, "PSG", now],
            ["Christiano Ronaldo", 37, 74, 187, "Al-Nassr", now],
            ["Christiano Ronaldo", 30, 74, 177, "Manchester United", now],
            [
                "Antony",
                22,
                69,
                139,
                "Manchester United",
                second_ahead,
            ],  # This will be skipped in salary join
        ]
        columns = ["name", "age", "height", "weight", "club", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "PlayerInfo", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            ["Manchester United", minute_ago, 1000000],
            ["PSG", minute_ago, 2000000],
            ["Al-Nassr", minute_ago, 3000000],
        ]
        columns = ["club", "timestamp", "salary"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "ClubSalary", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            [
                "Rashford",
                couple_minutes_ahead,
                "Lucia",
            ],  # This will be skipped in wag join
            ["Maguire", minute_ahead, "Fern"],
            ["Messi", minute_ahead, "Antonela"],
            ["Christiano Ronaldo", minute_ahead, "Georgina"],
            ["Antony", minute_ahead, "Rosilene"],
        ]
        columns = ["name", "timestamp", "wag"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "WAG", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        ts = pd.Series(
            [
                couple_minutes_ahead,
                couple_minutes_ahead,
                couple_minutes_ahead,
                couple_minutes_ahead,
                couple_minutes_ahead,
            ]
        )
        names = pd.Series(
            [
                "Rashford",
                "Maguire",
                "Messi",
                "Christiano Ronaldo",
                "Antony",
            ]
        )
        if client.is_integration_client():
            time.sleep(30)
        df, _ = ManchesterUnitedPlayerInfoBounded.lookup(ts, name=names)
        assert df.shape == (5, 8)
        assert df["club"].tolist() == [
            "Manchester United",
            "Manchester United",
            None,
            "Manchester United",
            "Manchester United",
        ]
        assert df["salary"].tolist() == [
            1000000,
            1000000,
            None,
            1000000,
            None,
        ]
        assert df["wag"].tolist() == [
            None,
            "Fern",
            None,
            "Georgina",
            "Rosilene",
        ]


@mock
def test_join(client):
    def test_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        assert df.shape == (3, 4), "Shape is not correct {}".format(df.shape)
        assert (
            "b1" not in df.columns
        ), "b1 column should not be present, " "{}".format(df.columns)
        return df

    @source(webhook.endpoint("A"))
    @meta(owner="aditya@fennel.ai")
    @dataset
    class A:
        a1: int = field(key=True)
        v: int
        t: datetime

    @source(webhook.endpoint("B"))
    @meta(owner="aditya@fennel.ai")
    @dataset
    class B:
        b1: int = field(key=True)
        v2: int
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDataset:
        a1: int = field(key=True)
        v: int
        v2: Optional[int]
        t: datetime

        @pipeline(version=1)
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            x = a.left_join(
                b,
                left_on=["a1"],
                right_on=["b1"],
            )  # type: ignore
            assert len(x.schema()) == 4
            x = x.transform(test_dataframe)  # type: ignore
            return x

    client.sync(datasets=[A, B, ABCDataset])

    now = datetime.now()
    df1 = pd.DataFrame(
        {
            "a1": [1, 2, 3],
            "v": [1, 2, 3],
            "t": [now, now, now],
        }
    )
    df2 = pd.DataFrame(
        {
            "b1": [1, 2, 4],
            "v2": [1, 2, 4],
            "t": [now, now, now],
        }
    )
    client.log("fennel_webhook", "A", df1)
    client.log("fennel_webhook", "B", df2)


def extract_payload(
    df: pd.DataFrame,
    payload_col: str = "payload",
    json_col: str = "json_payload",
) -> pd.DataFrame:
    df[json_col] = df[payload_col].apply(lambda x: json.loads(x))
    return df[["timestamp", json_col]]


def extract_keys(
    df: pd.DataFrame, json_col: str = "json_payload", keys: List[str] = []
) -> pd.DataFrame:
    for key in keys:
        df[key] = df[json_col].apply(lambda x: x[key])
        if key == "latitude" or key == "longitude":
            df[key] = df[key].astype(float)
    return df.drop(json_col, axis=1)


def extract_location_index(
    df: pd.DataFrame,
    index_col: str,
    latitude_col: str = "latitude",
    longitude_col: str = "longitude",
    resolution: int = 2,
) -> pd.DataFrame:
    df[index_col] = df.apply(
        lambda x: str(x[latitude_col])[0 : 3 + resolution]  # noqa
        + "-"  # noqa
        + str(x[longitude_col])[0 : 3 + resolution],  # noqa
        axis=1,
    )
    df[index_col] = df[index_col].astype(str)
    return df


@dataset
@source(webhook.endpoint("CommonEvent"))
@meta(owner="aditya@fennel.ai")
class CommonEvent:
    name: str
    timestamp: datetime = field(timestamp=True)
    payload: str


@dataset
@meta(owner="aditya@fennel.ai.com", description="Location index features")
class LocationLatLong:
    latlng2: str = field(key=True)
    timestamp: datetime = field(timestamp=True)
    onb_velocity_l1_2: int
    onb_velocity_l7_2: int
    onb_velocity_l30_2: int
    onb_velocity_l90_2: int
    onb_total_2: int

    @pipeline(version=1)
    @includes(extract_payload, extract_keys, extract_location_index)
    @inputs(CommonEvent)
    def get_payload(cls, common_event: Dataset):
        event = common_event.filter(lambda x: x["name"] == "LocationLatLong")
        new_schema = {
            "timestamp": datetime,
            "latitude": float,
            "longitude": float,
        }
        data = event.transform(
            extract_payload,
            schema={"json_payload": Dict[str, float], "timestamp": datetime},
        ).transform(
            lambda x: extract_keys(
                x,
                json_col="json_payload",
                keys=["latitude", "longitude"],
            ),
            schema=new_schema,
        )

        new_schema.update({"latlng2": str})
        data = data.transform(
            lambda x: extract_location_index(
                x, index_col="latlng2", resolution=2
            ),
            schema=new_schema,
        )

        return data.groupby("latlng2").aggregate(
            [
                Count(window=Window("1d"), into_field="onb_velocity_l1_2"),
                Count(window=Window("7d"), into_field="onb_velocity_l7_2"),
                Count(window=Window("30d"), into_field="onb_velocity_l30_2"),
                Count(window=Window("90d"), into_field="onb_velocity_l90_2"),
                Count(window=Window("forever"), into_field="onb_total_2"),
            ]
        )


def del_spaces_tabs_and_newlines(s):
    pattern = re.compile(r"^@meta.*$", re.MULTILINE)
    text = re.sub(pattern, "", s)
    return re.sub(r"[\s\n\t]+", "", text)


@pytest.mark.integration
@mock
def test_complex_lambda(client):
    # Test for code generation
    view = InternalTestClient()
    view.add(CommonEvent)
    view.add(LocationLatLong)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 2
    assert len(sync_request.operators) == 6

    expected_code = """
def extract_location_index(
        df: pd.DataFrame,
        index_col: str,
        latitude_col: str = "latitude",
        longitude_col: str = "longitude",
        resolution: int = 2,
) -> pd.DataFrame:
    df[index_col] = df.apply(
        lambda x: str(x[latitude_col])[0: 3 + resolution]  # noqa
                  + "-"  # noqa
                  + str(x[longitude_col])[0: 3 + resolution],  # noqa
        axis=1,
    )
    df[index_col] = df[index_col].astype(str)
    return df

def extract_keys(
        df: pd.DataFrame, json_col: str = "json_payload", keys: List[str] = []
) -> pd.DataFrame:
    for key in keys:
        df[key] = df[json_col].apply(lambda x: x[key])
        if key == "latitude" or key == "longitude":
            df[key] = df[key].astype(float)
    return df.drop(json_col, axis=1)


def extract_payload(
        df: pd.DataFrame,
        payload_col: str = "payload",
        json_col: str = "json_payload",
) -> pd.DataFrame:
    df[json_col] = df[payload_col].apply(lambda x: json.loads(x))
    return df[["timestamp", json_col]]

@dataset
@meta(owner="aditya@fennel.ai.com", description="Location index features")
class LocationLatLong:
    latlng2: str = field(key=True)
    timestamp: datetime = field(timestamp=True)
    onb_velocity_l1_2: int
    onb_velocity_l7_2: int
    onb_velocity_l30_2: int
    onb_velocity_l90_2: int
    onb_total_2: int

    @classmethod
    def wrapper_adace968e2(cls, *args, **kwargs):

        def extract_payload(
                df: pd.DataFrame,
                payload_col: str = "payload",
                json_col: str = "json_payload",
        ) -> pd.DataFrame:
            df[json_col] = df[payload_col].apply(lambda x: json.loads(x))
            return df[["timestamp", json_col]]


        return extract_payload(*args, **kwargs)


def LocationLatLong_wrapper_adace968e2(*args, **kwargs):
    _fennel_internal = LocationLatLong.__fennel_original_cls__
    return getattr(_fennel_internal, "wrapper_adace968e2")(*args, **kwargs)

"""

    assert del_spaces_tabs_and_newlines(
        sync_request.operators[2].transform.pycode.generated_code
    ) == del_spaces_tabs_and_newlines(expected_code)

    # Test running of code
    client.sync(datasets=[CommonEvent, LocationLatLong])

    data = [
        {
            "name": "LocationLatLong",
            "timestamp": datetime.now(),
            "payload": '{"user_id": "247", "latitude": 12.3, "longitude": 12.3, "token": "abc"}',
        },
        {
            "name": "LocationLatLong",
            "timestamp": datetime.now(),
            "payload": '{"user_id": "248", "latitude": 12.3, "longitude": 12.4, "token": "abc"}',
        },
        {
            "name": "LocationLatLong",
            "timestamp": datetime.now(),
            "payload": '{"user_id": "246", "latitude": 12.3, "longitude": 12.3, "token": "abc"}',
        },
    ]
    res = client.log("fennel_webhook", "CommonEvent", pd.DataFrame(data))
    assert res.status_code == 200, res.json()

    if client.is_integration_client():
        client.sleep()

    now = datetime.now()
    timestamps = pd.Series([now, now])
    res, found = LocationLatLong.lookup(
        ts=timestamps, latlng2=pd.Series(["12.3-12.4", "12.3-12.3"])
    )
    assert found.tolist() == [True, True]
    assert res.shape == (2, 7)
    assert res["onb_velocity_l1_2"].tolist() == [1, 2]


@dataset
@source(Webhook(name="fennel_webhook").endpoint("common"))
@meta(owner="nitin@epifi.com", tags=["common"])
class TransactionsCredit:
    updated_at: datetime = field(timestamp=True)
    to_account_id: str
    to_account_type: str
    payment_protocol: str
    provenance: str
    computed_amount: float


@dataset
@meta(owner="nitin@epifi.com", tags=["common"])
class TransactionsCreditInternetBanking:
    account_id: str = field(key=True)
    updated_at: datetime = field(timestamp=True)
    sum_credit_internet_banking: float
    min_credit_internet_banking: float
    max_credit_internet_banking: float
    mean_credit_internet_banking: float
    number_credit_internet_banking: int

    @pipeline(version=1)
    @inputs(TransactionsCredit)
    def get_payload(cls, event: Dataset):
        event = (
            event.rename({"to_account_id": "account_id"})  # type: ignore
            .filter(lambda x: x["provenance"] == "EXTERNAL")
            .filter(lambda x: x["payment_protocol"].isin(["RTGS", "IMPS"]))
            .groupby("account_id")
            .aggregate(
                [
                    Sum(
                        of="computed_amount",
                        window=Window("forever"),
                        into_field="sum_credit_internet_banking",
                    ),
                    Min(
                        of="computed_amount",
                        window=Window("forever"),
                        into_field="min_credit_internet_banking",
                        default=float("inf"),
                    ),
                    Max(
                        of="computed_amount",
                        window=Window("forever"),
                        into_field="max_credit_internet_banking",
                        default=float("-inf"),
                    ),
                    Average(
                        of="computed_amount",
                        window=Window("forever"),
                        into_field="mean_credit_internet_banking",
                    ),
                    Count(
                        window=Window("forever"),
                        into_field="number_credit_internet_banking",
                    ),
                ]
            )
        )
        return event


@mock
def test_chained_lambda(client):
    client.sync(
        datasets=[TransactionsCredit, TransactionsCreditInternetBanking]
    )
    view = InternalTestClient()
    view.add(TransactionsCredit)
    view.add(TransactionsCreditInternetBanking)
    sync_request = view._get_sync_request_proto()

    assert len(sync_request.pipelines) == 1
    assert len(sync_request.operators) == 5

    expected_code = """
@dataset
@meta(owner="nitin@epifi.com", tags=["common"])
class TransactionsCreditInternetBanking:
    account_id: str = field(key=True)
    updated_at: datetime = field(timestamp=True)
    sum_credit_internet_banking: float
    min_credit_internet_banking: float
    max_credit_internet_banking: float
    mean_credit_internet_banking: float
    number_credit_internet_banking: int


    @classmethod
    def wrapper_4d45b34b11(cls, *args, **kwargs):
        _fennel_internal = lambda x: x["payment_protocol"].isin(["RTGS", "IMPS"])
        return _fennel_internal(*args, **kwargs)


def TransactionsCreditInternetBanking_wrapper_4d45b34b11(*args, **kwargs):
    _fennel_internal = TransactionsCreditInternetBanking.__fennel_original_cls__
    return getattr(_fennel_internal, "wrapper_4d45b34b11")(*args, **kwargs)

def TransactionsCreditInternetBanking_wrapper_4d45b34b11_filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[TransactionsCreditInternetBanking_wrapper_4d45b34b11(df)]
    """

    assert del_spaces_tabs_and_newlines(
        sync_request.operators[3].filter.pycode.generated_code
    ) == del_spaces_tabs_and_newlines(expected_code)
