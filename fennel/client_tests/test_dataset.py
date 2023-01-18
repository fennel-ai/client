import json
import time
import unittest
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import pytest
import requests

from fennel.datasets import dataset, field, pipeline, Dataset, on_demand
from fennel.lib.aggregate import Count, Sum, Average
from fennel.lib.metadata import meta
from fennel.lib.schema import Embedding, Series
from fennel.lib.window import Window
from fennel.test_lib import mock_client


################################################################################
#                           Dataset Unit Tests
################################################################################


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


class TestDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_sync_dataset(self, client):
        """Sync the dataset and check if it is synced correctly."""
        # Sync the dataset
        response = client.sync(datasets=[UserInfoDataset])
        assert response.status_code == requests.codes.OK, response.json()

    @pytest.mark.integration
    @mock_client
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
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

    @pytest.mark.integration
    @mock_client
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
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        data = [
            [18232, "Ross", "32", "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        if client.is_integration_client():
            assert (
                response.json()["error"]
                == """error: input parse error: expected Int, but got String("32")"""
            )
        else:
            assert (
                response.json()["error"]
                == "[ValueError('Field age is of type int, but the column in the dataframe is of type object.')]"
            )
        # Do some lookups
        user_ids = pd.Series([18232, 18234, 1920])
        ts = pd.Series([now, now, now])
        df, found = UserInfoDataset.lookup(
            ts, user_id=user_ids, fields=["name", "age", "country"]
        )
        assert found.tolist() == [True, True, False]
        assert df["name"].tolist() == ["Ross", "Monica", None]
        assert df["age"].tolist() == [32, 24, None]
        assert df["country"].tolist() == ["USA", "Chile", None]
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
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK
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
    @mock_client
    def test_invalid_dataschema(self, client):
        """Check if invalid data raises an error."""
        client.sync(datasets=[UserInfoDataset])
        data = [
            [18232, "Ross", "32", "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        assert len(response.json()["error"]) > 0

    @pytest.mark.integration
    @mock_client
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


class TestDocumentDataset(unittest.TestCase):
    @mock_client
    def test_log_to_document_dataset(self, client):
        """Log some data to the dataset and check if it is logged correctly."""

        @meta(owner="aditya@fennel.ai")
        @dataset
        class DocumentContentDataset:
            doc_id: int = field(key=True)
            bert_embedding: Embedding[4]
            fast_text_embedding: Embedding[3]
            num_words: int
            timestamp: datetime = field(timestamp=True)

            @on_demand(expires_after="3d")
            def get_embedding(cls, ts: Series[datetime], doc_ids: Series[int]):
                data = []
                doc_ids = doc_ids.tolist()
                for i in range(len(ts)):
                    data.append(
                        [
                            doc_ids[i],
                            [0.1, 0.2, 0.3, 0.4],
                            [1.1, 1.2, 1.3],
                            10 * i,
                            ts[i],
                        ]
                    )
                columns = [
                    str(cls.doc_id),
                    str(cls.bert_embedding),
                    str(cls.fast_text_embedding),
                    str(cls.num_words),
                    str(cls.timestamp),
                ]
                return pd.DataFrame(data, columns=columns), pd.Series(
                    [True] * len(ts)
                )

        # Sync the dataset
        client.sync(datasets=[DocumentContentDataset])
        now = datetime.now()
        data = [
            [18232, np.array([1, 2, 3, 4]), np.array([1, 2, 3]), 10, now],
            [
                18234,
                np.array([1, 2.2, 0.213, 0.343]),
                np.array([0.87, 2, 3]),
                9,
                now,
            ],
            [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
        ]
        columns = [
            "doc_id",
            "bert_embedding",
            "fast_text_embedding",
            "num_words",
            "timestamp",
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("DocumentContentDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do some lookups
        doc_ids = pd.Series([18232, 1728, 18234, 18934, 19200, 91012])
        ts = pd.Series([now, now, now, now, now, now])
        df, _ = DocumentContentDataset.lookup(ts, doc_id=doc_ids)
        assert df.shape == (6, 5)
        assert df["num_words"].tolist() == [10.0, 9.0, 12, 0, 10.0, 20.0]


################################################################################
#                           Dataset & Pipelines Unit Tests
################################################################################


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

    @pipeline(id=1)
    def pipeline_aggregate(cls, activity: Dataset[RatingActivity]):
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
            ]
        )


@meta(owner="test@test.com")
@dataset
class MovieRatingTransformed:
    movie: str = field(key=True)
    rating_sq: float
    rating_cube: float
    rating_into_5: float
    t: datetime

    @pipeline(id=1)
    def pipeline_transform(cls, m: Dataset[MovieRating]):
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
                "movie": str,
                "t": datetime,
                "rating_sq": float,
                "rating_cube": float,
                "rating_into_5": float,
            },
        )


class TestBasicTransform(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
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
        response = client.log("MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()
        if client.is_integration_client():
            time.sleep(3)
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
        if client.is_integration_client():
            time.sleep(3)
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
@dataset
class MovieRevenue:
    movie: str = field(key=True)
    revenue: int
    t: datetime


@meta(owner="aditya@fennel.ai")
@dataset
class MovieStats:
    movie: str = field(key=True)
    rating: float
    revenue_in_millions: float
    t: datetime

    @pipeline(id=1)
    def pipeline_join(
        cls, rating: Dataset[MovieRating], revenue: Dataset[MovieRevenue]
    ):
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
                str(cls.movie): str,
                str(cls.rating): float,
                str(cls.t): datetime,
                str(cls.revenue_in_millions): float,
            },
        )


class TestBasicJoin(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
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
        response = client.log("MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()

        two_hours_ago = now - timedelta(hours=2)
        data = [["Jumanji", 2000000, two_hours_ago], ["Titanic", 50000000, now]]
        columns = ["movie", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRevenue", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do some lookups to verify pipeline_join is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        if client.is_integration_client():
            time.sleep(3)
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
    @mock_client
    def test_basic_aggregate(self, client):
        # # Sync the dataset
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

        if client.is_integration_client():
            time.sleep(3)

        # Do some lookups to verify pipeline_aggregate is working as expected
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


@meta(owner="test@test.com")
@dataset
class MovieRatingWindowed:
    movie: str = field(key=True)
    num_ratings_3d: int
    sum_ratings_7d: float
    avg_rating_6h: float
    total_ratings: int
    t: datetime

    @pipeline(id=1)
    def pipeline_aggregate(cls, activity: Dataset[RatingActivity]):
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
    @mock_client
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
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()
        if client.is_integration_client():
            time.sleep(3)

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


class TestE2EPipeline(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_e2e_pipeline(self, client):
        """We enter ratings activity and we get movie stats."""
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, MovieRevenue, RatingActivity, MovieStats],
        )
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)
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
            [18231, 5, "Titanic", minute_ago],
            [18231, 3, "Titanic", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        data = [
            ["Jumanji", 1000000, five_hours_ago],
            ["Titanic", 50000000, three_hours_ago],
        ]
        columns = ["movie", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRevenue", df)
        assert response.status_code == requests.codes.OK
        if client.is_integration_client():
            time.sleep(3)

        # Do some lookups to verify data flow is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieStats.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (2, 4)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["revenue_in_millions"].tolist() == [1, 50]


@meta(owner="test@test.com")
@dataset
class PositiveRatingActivity:
    cnt_rating: int
    movie: str = field(key=True)
    t: datetime

    @pipeline(id=1)
    def filter_positive_ratings(cls, rating: Dataset[RatingActivity]):
        filtered_ds = rating.filter(lambda df: df[df["rating"] >= 3.5])
        return filtered_ds.groupby("movie").aggregate(
            [Count(window=Window("forever"), into_field=str(cls.cnt_rating))],
        )


class TestBasicFilter(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
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
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK
        if client.is_integration_client():
            time.sleep(3)

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
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="me@fenne.ai")
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
    # merchant_id: int
    # location: str
    # transaction_amount: float

    num_categ_fraudulent_transactions: int
    num_categ_fraudulent_transactions_7d: int
    sum_categ_fraudulent_transactions_7d: float

    @pipeline(id=1)
    def create_fraud_dataset(
        cls, activity: Dataset[Activity], merchant_info: Dataset[MerchantInfo]
    ):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df_timestamp = pd.concat([df_json, df["timestamp"]], axis=1)
            return df_timestamp

        def fillna(df: pd.DataFrame) -> pd.DataFrame:
            df["category"].fillna("unknown", inplace=True)
            return df

        filtered_ds = activity.filter(
            lambda df: df[df["action_type"] == "report"]
        )
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
    @mock_client
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
        response = client.log("Activity", df)
        assert response.status_code == requests.codes.OK, response.json()

        two_days_ago = now - timedelta(days=2)
        data = [
            [1322, "grocery", "mumbai", two_days_ago],
            [1422, "entertainment", "delhi", two_days_ago],
        ]

        columns = ["merchant_id", "category", "location", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MerchantInfo", df)
        assert response.status_code == requests.codes.OK, response.json()

        if client.is_integration_client():
            time.sleep(3)
        now = datetime.now()
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


@meta(owner="me@fennel.ai")
@dataset
class UserAge:
    name: str = field(key=True)
    age: int
    city: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
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

    @pipeline(id=1)
    def create_user_age_aggregated(cls, user_age: Dataset[UserAge]):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )

    @pipeline(id=2)
    def create_user_age_aggregated2(cls, user_age: Dataset[UserAgeNonTable]):
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
    @mock_client
    def test_table_aggregation(self, client):
        client.sync(datasets=[UserAge, UserAgeNonTable, UserAgeAggregated])
        yesterday = datetime.now() - timedelta(days=1)
        now = datetime.now()
        tomorrow = datetime.now() + timedelta(days=1)

        data = [
            ["Sonu", 24, "mumbai", yesterday],
            ["Monu", 25, "delhi", yesterday],
        ]

        columns = ["name", "age", "city", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        ts = pd.Series([now, now])
        names = pd.Series(["mumbai", "delhi"])
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        input_df["timestamp"] = tomorrow
        response = client.log("UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df.shape == (2, 3)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        # Change the age of Sonu and Monu
        input_df["age"] = [30, 40]
        two_days_from_now = datetime.now() + timedelta(days=2)
        input_df["timestamp"] = two_days_from_now
        response = client.log("UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        if client.is_integration_client():
            return

        three_days_from_now = datetime.now() + timedelta(days=3)
        ts = pd.Series([three_days_from_now, three_days_from_now])
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df.shape == (2, 3)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        # The value has updated from [24, 25] to [30, 40]
        # since UserAge is keyed on the name hence the aggregate
        # value is updated from [24, 25] to [30, 40]
        assert df["sum_age"].tolist() == [30, 40]

        four_days_from_now = datetime.now() + timedelta(days=4)
        input_df["timestamp"] = four_days_from_now
        response = client.log("UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        five_days_from_now = datetime.now() + timedelta(days=5)
        ts = pd.Series([five_days_from_now, five_days_from_now])
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df.shape == (2, 3)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        # The value has NOT updated but increased from
        # [24, 25] to [24+30, 25+40]
        assert df["sum_age"].tolist() == [54, 65]


################################################################################
#                           Dataset & Pipelines Complex E2E Tests
################################################################################


@meta(owner="gianni@fifa.com")
@dataset
class PlayerInfo:
    name: str = field(key=True)
    age: int
    height: int = field().meta(description="in inches")  # type: ignore
    weight: int = field().meta(description="in pounds")  # type: ignore
    club: str
    timestamp: datetime


@meta(owner="gianni@fifa.com")
@dataset
class ClubSalary:
    club: str = field(key=True)
    timestamp: datetime
    salary: int


@meta(owner="gianni@fifa.com")
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

    @pipeline(id=1)
    def create_player_detailed_info(
        cls,
        player_info: Dataset[PlayerInfo],
        club_salary: Dataset[ClubSalary],
        wag: Dataset[WAG],
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
            club_salary, on=["club"]
        )
        manchester_players = player_info_with_salary.filter(
            lambda df: df[df["club"] == "Manchester United"]
        )
        return manchester_players.left_join(wag, on=["name"])


class TestE2eIntegrationTestMUInfo(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
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
        response = client.log("PlayerInfo", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        if client.is_integration_client():
            time.sleep(3)
        data = [
            ["Manchester United", yesterday, 1000000],
            ["PSG", yesterday, 2000000],
            ["Al-Nassr", yesterday, 3000000],
        ]
        columns = ["club", "timestamp", "salary"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("ClubSalary", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        if client.is_integration_client():
            time.sleep(3)
        data = [
            ["Rashford", yesterday, "Lucia"],
            ["Maguire", yesterday, "Fern"],
            ["Messi", yesterday, "Antonela"],
            ["Christiano Ronaldo", yesterday, "Georgina"],
            ["Antony", yesterday, "Rosilene"],
        ]
        columns = ["name", "timestamp", "wag"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("WAG", input_df)
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
        if client.is_integration_client():
            time.sleep(5)
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
