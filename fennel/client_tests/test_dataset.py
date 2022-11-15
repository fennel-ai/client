import unittest
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Average, Count, Sum
from fennel.lib.metadata import meta
from fennel.lib.window import Window
from fennel.test_lib import mock_client


################################################################################
#                           Dataset Unit Tests
################################################################################


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


class TestDataset(unittest.TestCase):
    @mock_client
    def test_log_to_dataset(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # # Sync the dataset
        client.sync(datasets=[UserInfoDataset])

        data = [
            [18232, "Ross", 32, "USA", 1668475993],
            [18234, "Monica", 24, "Chile", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups
        user_ids = pd.Series([18232, 18234])
        ts = pd.Series([1668500000, 1668500000])
        df = UserInfoDataset.lookup(ts, user_id=user_ids)

        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["age"].tolist() == [32, 24]
        assert df["country"].tolist() == ["USA", "Chile"]

        # Do some lookups with a timestamp
        ts = pd.Series([1668475343, 1668475343])
        df = UserInfoDataset.lookup(ts, user_id=user_ids)

        assert df["name"].tolist() == [None, "Monica"]
        assert df["age"].tolist() == [None, 24]
        assert df["country"].tolist() == [None, "Chile"]

        data = [
            [18232, "Ross", 33, "Russia", 1668500001],
            [18234, "Monica", 25, "Columbia", 1668500004],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups
        ts = pd.Series([1668500001, 1668500001])
        df = UserInfoDataset.lookup(ts, user_id=user_ids)
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["age"].tolist() == [33, 24]
        assert df["country"].tolist() == ["Russia", "Chile"]

        ts = pd.Series([1668500004, 1668500004])
        df = UserInfoDataset.lookup(ts, user_id=user_ids)
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["age"].tolist() == [33, 25]
        assert df["country"].tolist() == ["Russia", "Columbia"]

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
    name: str = field(key=True)
    rating: float
    num_ratings: int
    sum_ratings: float
    t: datetime

    @staticmethod
    @pipeline(RatingActivity)
    def pipeline_aggregate(activity: Dataset):
        def rename(df: pd.DataFrame) -> pd.DataFrame:
            df = df.rename(columns={"movie": "name"})
            return df

        ds = activity.groupby("movie").aggregate(
            [
                Count(window=Window(), name="num_ratings"),
                Sum(window=Window(), value="rating", name="sum_ratings"),
                Average(window=Window(), value="rating", name="rating"),
            ]
        )
        return ds.transform(rename)


@meta(owner="test@test.com")
@dataset
class MovieRatingTransformed:
    name: str = field(key=True)
    rating_sq: float
    rating_cube: float
    rating_into_5: float
    t: datetime

    @staticmethod
    @pipeline(MovieRating)
    def pipeline_transform(m: Dataset):
        def t(df: pd.DataFrame) -> pd.DataFrame:
            df["rating_sq"] = df["rating"] * df["rating"]
            df["rating_cube"] = df["rating_sq"] * df["rating"]
            df["rating_into_5"] = df["rating"] * 5
            return df

        return m.transform(t)


class TestBasicTransform(unittest.TestCase):
    @mock_client
    def test_basic_transform(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, MovieRatingTransformed],
        )
        data = [["Jumanji", 4, 343, 789, 1000], ["Titanic", 5, 729, 1232, 1002]]
        columns = ["name", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRating", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify pipeline_transform is working as expected
        ts = pd.Series([1000, 1000])
        names = pd.Series(["Jumanji", "Titanic"])
        df = MovieRatingTransformed.lookup(
            ts,
            names=names,
        )
        assert df.shape == (2, 4)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, None]
        assert df["rating_cube"].tolist() == [64, None]
        assert df["rating_into_5"].tolist() == [20, None]

        ts = pd.Series([1002, 1002])
        names = pd.Series(["Jumanji", "Titanic"])
        df = MovieRatingTransformed.lookup(
            ts,
            names=names,
        )
        assert df.shape == (2, 4)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, 25]
        assert df["rating_cube"].tolist() == [64, 125]
        assert df["rating_into_5"].tolist() == [20, 25]


@meta(owner="test@test.com")
@dataset
class MovieRevenue:
    name: str = field(key=True)
    revenue: int
    t: datetime


@meta(owner="aditya@fennel.ai")
@dataset
class MovieStats:
    name: str = field(key=True)
    rating: float
    revenue_in_millions: int
    t: datetime

    @staticmethod
    @pipeline(MovieRating, MovieRevenue)
    def pipeline_join(rating: Dataset, revenue: Dataset):
        def to_millions(df: pd.DataFrame) -> pd.DataFrame:
            df["revenue_in_millions"] = df["revenue"] / 1000000
            return df

        c = rating.join(revenue, on=["name"])
        return c.transform(to_millions)


class TestBasicJoin(unittest.TestCase):
    @mock_client
    def test_basic_join(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, MovieRevenue, MovieStats],
        )
        data = [["Jumanji", 4, 343, 789, 1000], ["Titanic", 5, 729, 1232, 1002]]
        columns = ["name", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRating", df)
        assert response.status_code == requests.codes.OK

        data = [["Jumanji", 1000000, 1000], ["Titanic", 50000000, 1002]]
        columns = ["name", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRevenue", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify pipeline_join is working as expected
        ts = pd.Series([1002, 1002])
        names = pd.Series(["Jumanji", "Titanic"])
        df = MovieStats.lookup(
            ts,
            names=names,
        )
        assert df.shape == (2, 3)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [4, 5]
        assert df["revenue_in_millions"].tolist() == [1, 50]


class TestBasicAggregate(unittest.TestCase):
    @mock_client
    def test_basic_aggregate(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, RatingActivity],
        )
        data = [
            [18231, 2, "Jumanji", 1000],
            [18231, 3, "Jumanji", 1001],
            [18231, 2, "Jumanji", 1002],
            [18231, 5, "Jumanji", 1000],
            [18231, 4, "Titanic", 1002],
            [18231, 3, "Titanic", 1003],
            [18231, 5, "Titanic", 1004],
            [18231, 5, "Titanic", 1005],
            [18231, 3, "Titanic", 1003],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series([1005, 1005])
        names = pd.Series(["Jumanji", "Titanic"])
        df = MovieRating.lookup(
            ts,
            names=names,
        )
        assert df.shape == (2, 4)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]


class TestE2EPipeline(unittest.TestCase):
    @mock_client
    def test_e2e_pipeline(self, client):
        """We enter ratings activity and we get movie stats."""
        # # Sync the dataset
        client.sync(
            datasets=[MovieRating, MovieRevenue, RatingActivity, MovieStats],
        )
        data = [
            [18231, 2, "Jumanji", 1000],
            [18231, 3, "Jumanji", 1001],
            [18231, 2, "Jumanji", 1002],
            [18231, 5, "Jumanji", 1000],
            [18231, 4, "Titanic", 1002],
            [18231, 3, "Titanic", 1003],
            [18231, 5, "Titanic", 1004],
            [18231, 5, "Titanic", 1005],
            [18231, 3, "Titanic", 1003],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK

        data = [["Jumanji", 1000000, 1000], ["Titanic", 50000000, 1002]]
        columns = ["name", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRevenue", df)
        print(response.json())
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify data flow is working as expected
        ts = pd.Series([1005, 1005])
        names = pd.Series(["Jumanji", "Titanic"])
        df = MovieStats.lookup(
            ts,
            names=names,
        )
        print(df)
        assert df.shape == (2, 3)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["revenue_in_millions"].tolist() == [1, 50]


################################################################################
#                           Dataset & Pipelines Complex Unit Tests
################################################################################

"""
@meta(owner="me@fennel.ai")
@dataset(retention="4m")
class Activity:
    user_id: int
    action_type: float
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset
class FraudReportAggregatedDataset:
    merchant_id: int = field(key=True)
    timestamp: datetime
    num_merchant_fraudulent_transactions: int
    num_merchant_fraudulent_transactions_7d: int

    @pipeline(Activity, UserInfoDataset)
    def create_fraud_dataset(activity: Dataset, user_info: Dataset):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df['metadata_dict'] = df['metadata'].apply(json.loads).apply(
                pd.Series)
            df['transaction_amount'] = df['metadata_dict'].apply(
                lambda x: x['transaction_amt'])
            df['timestamp'] = df['metadata_dict'].apply(
                lambda x: x['transaction_amt'])
            df['merchant_id'] = df['metadata_dict'].apply(
                lambda x: x['merchant_id'])
            return df[
                ['merchant_id', 'transaction_amount', 'user_id', 'timestamp']]

        filtered_ds = activity.transform(
            lambda df: df[df["action_type"] == "report_txn"]
        )
        ds = filtered_ds.join(user_info, on=["user_id"], )
        ds = ds.transform(extract_info)
        return ds.groupby("merchant_id").aggregate([
            Count(window=Window(), name="num_merchant_fraudulent_transactions"),
            Count(window=Window("1w"),
                name="num_merchant_fraudulent_transactions_7d"),
        ])


class TestDatasetAndPipelines(unittest.TestCase):
    @mock_client
    def test_log_to_dataset(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[UserInfoDataset, Activity, FraudReportAggregatedDataset])

        data = [
            [18232, "Ross", 32, "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK
"""
