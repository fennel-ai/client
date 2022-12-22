import json
import time
import unittest
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
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
    @mock_client
    def test_sync_dataset(self, client):
        """Sync the dataset and check if it is synced correctly."""
        # Sync the dataset
        response = client.sync(datasets=[UserInfoDataset])
        assert response.status_code == requests.codes.OK, response.json()

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
        time.sleep(2)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        data = [
            [18232, "Ross", "32", "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        assert (
            response.json()["error"]
            == "[ValueError('Field age is of type int, but the column in the dataframe is of type object.')]"
        )

        # Do some lookups
        user_ids = pd.Series([18232, 18234, 1920])
        ts = pd.Series([now, now, now])
        df, found = UserInfoDataset.lookup(ts, user_id=user_ids)
        assert found.tolist() == [True, True, False]
        assert df["name"].tolist() == ["Ross", "Monica", None]
        assert df["age"].tolist() == [32, 24, None]
        assert df["country"].tolist() == ["USA", "Chile", None]

        # Do some lookups with a timestamp
        user_ids = pd.Series([18232, 18234])
        six_hours_ago = now - pd.Timedelta(hours=6)
        ts = pd.Series([six_hours_ago, six_hours_ago])
        df, found = UserInfoDataset.lookup(ts, user_id=user_ids)
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
        df, found = UserInfoDataset.lookup(ts, user_id=user_ids)
        assert found.tolist() == [True, True]
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18234, 18232]
        assert df["age"].tolist() == [24, 33]
        assert df["country"].tolist() == ["Chile", "Russia"]

        ts = pd.Series([three_days_from_now, three_days_from_now])
        df, lookup = UserInfoDataset.lookup(ts, user_id=user_ids)
        assert lookup.tolist() == [True, True]
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


@meta(owner="aditya@fennel.ai")
@dataset
class DocumentContentDataset:
    doc_id: int = field(key=True)
    bert_embedding: Embedding[4]
    fast_text_embedding: Embedding[3]
    num_words: int
    timestamp: datetime = field(timestamp=True)

    @on_demand(expires_after="3d")
    def get_embedding(ts: Series[datetime], doc_ids: Series[int]):
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
            "doc_id",
            "bert_embedding",
            "fast_text_embedding",
            "num_words",
            "timestamp",
        ]
        return pd.DataFrame(data, columns=columns), pd.Series([True] * len(ts))


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
            def get_embedding(ts: Series[datetime], doc_ids: Series[int]):
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
                    "doc_id",
                    "bert_embedding",
                    "fast_text_embedding",
                    "num_words",
                    "timestamp",
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
        assert df.shape == (6, 4)
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
                Count(window=Window("forever"), into_field="num_ratings"),
                Sum(
                    window=Window("forever"),
                    of="rating",
                    into_field="sum_ratings",
                ),
                Average(
                    window=Window("forever"), of="rating", into_field="rating"
                ),
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
        now = datetime.now()
        two_hours_ago = now - timedelta(hours=2)
        data = [
            ["Jumanji", 4, 343, 789, two_hours_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["name", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do some lookups to verify pipeline_transform is working as expected
        an_hour_ago = now - timedelta(hours=1)
        ts = pd.Series([an_hour_ago, an_hour_ago])
        names = pd.Series(["Jumanji", "Titanic"])
        df, found = MovieRatingTransformed.lookup(
            ts,
            names=names,
        )
        assert found.tolist() == [True, False]
        assert df.shape == (2, 4)
        assert df["name"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, None]
        assert df["rating_cube"].tolist() == [64, None]
        assert df["rating_into_5"].tolist() == [20, None]

        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieRatingTransformed.lookup(
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
    revenue_in_millions: float
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
        now = datetime.now()
        two_hours_ago = now - timedelta(hours=2)
        data = [
            ["Jumanji", 4, 343, 789, two_hours_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["name", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()

        data = [["Jumanji", 1000000, two_hours_ago], ["Titanic", 50000000, now]]
        columns = ["name", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRevenue", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do some lookups to verify pipeline_join is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieStats.lookup(
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
            [18231, 5, "Titanic", now],
            [18231, 3, "Titanic", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("RatingActivity", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieRating.lookup(
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
            [18231, 5, "Titanic", now],
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
        columns = ["name", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("MovieRevenue", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups to verify data flow is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieStats.lookup(
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
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df_timestamp = pd.concat([df_json, df["timestamp"]], axis=1)
            return df_timestamp

        filtered_ds = activity.filter(
            lambda df: df[df["action_type"] == "report"]
        )
        ds = filtered_ds.transform(extract_info)
        ds = ds.join(
            merchant_info,
            on=["merchant_id"],
        )
        aggregated_ds = ds.groupby("category").aggregate(
            [
                Count(
                    window=Window("forever"),
                    into_field="num_categ_fraudulent_transactions",
                ),
                Count(
                    window=Window("1w"),
                    into_field="num_categ_fraudulent_transactions_7d",
                ),
                Sum(
                    window=Window("1w"),
                    of="transaction_amount",
                    into_field="sum_categ_fraudulent_transactions_7d",
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
        now = datetime.now()
        data = [
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322}',
                now,
            ],
            [
                13423,
                "atm_withdrawal",
                99,
                '{"location": "mumbai"}',
                now,
            ],
            [
                14325,
                "report",
                99,
                '{"transaction_amount": 99, "merchant_id": 1422}',
                now,
            ],
            [
                18347,
                "atm_withdrawal",
                209,
                '{"location": "delhi"}',
                now,
            ],
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322}',
                now,
            ],
            [
                18232,
                "report",
                149,
                '{"transaction_amount": 149, "merchant_id": 1422}',
                now,
            ],
            [
                18232,
                "report",
                999,
                '{"transaction_amount": 999, "merchant_id": 1322}',
                now,
            ],
            [
                18232,
                "report",
                199,
                '{"transaction_amount": 199, "merchant_id": 1322}',
                now,
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

        ts = pd.Series([now, now])
        categories = pd.Series(["grocery", "entertainment"])
        df, _ = FraudReportAggregatedDataset.lookup(
            ts, merchant_categ=categories
        )
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
                    of="age",
                    into_field="sum_age",
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
                    of="age",
                    into_field="sum_age",
                )
            ]
        )


class TestAggregateTableDataset(unittest.TestCase):
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
        df, _ = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        input_df["timestamp"] = tomorrow
        response = client.log("UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        df, _ = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        # Change the age of Sonu and Monu
        input_df["age"] = [30, 40]
        two_days_from_now = datetime.now() + timedelta(days=2)
        input_df["timestamp"] = two_days_from_now
        response = client.log("UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        three_days_from_now = datetime.now() + timedelta(days=3)
        ts = pd.Series([three_days_from_now, three_days_from_now])
        df, _ = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
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
        df, _ = UserAgeAggregated.lookup(ts, name=names)
        assert df.shape == (2, 2)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        # The value has NOT updated but increased from
        # [24, 25] to [24+30, 25+40]
        assert df["sum_age"].tolist() == [54, 65]
