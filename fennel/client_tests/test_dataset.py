import json
import unittest
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Count, Sum, Average
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
        # Sync the dataset
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

    #
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
        ds = activity.groupby("movie").aggregate(
            [
                Count(window=Window(), name="num_ratings"),
                Sum(window=Window(), value="rating", name="sum_ratings"),
                Average(window=Window(), value="rating", name="rating"),
            ]
        )
        return ds.transform(lambda df: df.rename(columns={"movie": "name"}))


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
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify data flow is working as expected
        ts = pd.Series([1005, 1005])
        names = pd.Series(["Jumanji", "Titanic"])
        df = MovieStats.lookup(
            ts,
            names=names,
        )

        assert df.shape == (2, 3)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["revenue_in_millions"].tolist() == [1, 50]


################################################################################
#                           Dataset & Pipelines Complex Unit Tests
################################################################################


@meta(owner="me@fennel.ai")
@dataset(retention="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="me@fenne.ai")
@dataset(retention="4m")
class MerchantInfo:
    merchant_id: int
    category: str
    location: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset
class FraudReportAggregatedDataset:
    merchant_categ: str = field(key=True)
    timestamp: datetime
    num_categ_fraudulent_transactions: int
    num_categ_fraudulent_transactions_7d: int
    sum_categ_fraudulent_transactions_7d: int

    @staticmethod
    @pipeline(Activity, MerchantInfo)
    def create_fraud_dataset(activity: Dataset, merchant_info: Dataset):
        filtered_ds = activity.transform(
            lambda df: df[df["action_type"] == "report"]
        )
        ds = filtered_ds.transform(
            lambda df: df["metadata"].apply(json.loads).apply(pd.Series)
        )
        ds = ds.join(
            merchant_info,
            on=["merchant_id"],
        )
        aggregated_ds = ds.groupby("category").aggregate(
            [
                Count(
                    window=Window(), name="num_categ_fraudulent_transactions"
                ),
                Count(
                    window=Window("1w"),
                    name="num_categ_fraudulent_transactions_7d",
                ),
                Sum(
                    window=Window("1w"),
                    value="transaction_amount",
                    name="sum_categ_fraudulent_transactions_7d",
                ),
            ]
        )
        return aggregated_ds.transform(
            lambda df: df.rename(columns={"category": "merchant_categ"})
        )


class TestFraudReportAggregatedDataset(unittest.TestCase):
    @mock_client
    def test_fraud(self, client):
        # # Sync the dataset
        client.sync(
            datasets=[MerchantInfo, Activity, FraudReportAggregatedDataset]
        )
        data = [
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322, "timestamp": 999}',
                1001,
            ],
            [
                13423,
                "atm_withdrawal",
                99,
                '{"location": "mumbai", "timestamp": 1299}',
                1001,
            ],
            [
                14325,
                "report",
                99,
                '{"transaction_amount": 99, "merchant_id": 1422, "timestamp": 999}',
                1001,
            ],
            [
                18347,
                "atm_withdrawal",
                209,
                '{"location": "delhi", "timestamp": 999}',
                1001,
            ],
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322, "timestamp": 999}',
                1001,
            ],
            [
                18232,
                "report",
                149,
                '{"transaction_amount": 149, "merchant_id": 1422, "timestamp": '
                "999}",
                1001,
            ],
            [
                18232,
                "report",
                999,
                '{"transaction_amount": 999, "merchant_id": 1322, "timestamp": '
                "999}",
                1001,
            ],
            [
                18232,
                "report",
                199,
                '{"transaction_amount": 199, "merchant_id": 1322, "timestamp": '
                "1002}",
                1001,
            ],
        ]
        columns = ["user_id", "action_type", "amount", "metadata", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("Activity", df)
        assert response.status_code == requests.codes.OK, response.json()

        data = [
            [1322, "grocery", "mumbai", 501],
            [1422, "entertainment", "delhi", 501],
        ]

        columns = ["merchant_id", "category", "location", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MerchantInfo", df)
        assert response.status_code == requests.codes.OK, response.json()

        ts = pd.Series([3001, 3001])
        categories = pd.Series(["grocery", "entertainment"])
        df = FraudReportAggregatedDataset.lookup(ts, merchant_categ=categories)
        assert df.shape == (2, 4)
        assert df["merchant_categ"].tolist() == ["grocery", "entertainment"]
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

    @staticmethod
    @pipeline(UserAge)
    def create_user_age_aggregated(user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    value="age",
                    name="sum_age",
                )
            ]
        )

    @staticmethod
    @pipeline(UserAgeNonTable)
    def create_user_age_aggregated2(user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    value="age",
                    name="sum_age",
                )
            ]
        )


class TestAggregateTableDataset(unittest.TestCase):
    @mock_client
    def test_table_aggregation(self, client):
        client.sync(datasets=[UserAge, UserAgeNonTable, UserAgeAggregated])
        data = [
            ["Sonu", 24, "mumbai", 500],
            ["Monu", 25, "delhi", 500],
        ]

        columns = ["name", "age", "city", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        ts = pd.Series([501, 501])
        names = pd.Series(["mumbai", "delhi"])
        df = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        input_df["timestamp"] = 502
        response = client.log("UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        df = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        # Change the age of Sonu and Monu
        input_df["age"] = [30, 40]
        input_df["timestamp"] = 503
        response = client.log("UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        ts = pd.Series([504, 504])
        df = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        # The value has updated from [24, 25] to [30, 40]
        # since UserAge is keyed on the name hence the aggregate
        # value is updated from [24, 25] to [30, 40]
        assert df["sum_age"].tolist() == [30, 40]

        input_df["timestamp"] = 505
        response = client.log("UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        ts = pd.Series([506, 506])
        df = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        # The value has NOT updated but increased from
        # [24, 25] to [24+30, 25+40]
        assert df["sum_age"].tolist() == [54, 65]
