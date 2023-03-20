import pandas as pd
import json
import requests

from datetime import datetime, timedelta

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.test_lib import mock_client
from typing import List, Dict, Tuple, Optional


# docsnip data_sets
@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    signup_time: datetime = field(timestamp=True)


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class Transaction:
    uid: int
    amount: float
    payment_country: str
    merchant_id: int
    timestamp: datetime


# /docsnip


# docsnip pipeline
from fennel.datasets import pipeline, Dataset
from fennel.lib.aggregate import Count, Sum
from fennel.lib.window import Window


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class UserTransactionsAbroad:
    uid: int = field(key=True)
    count: int
    amount_1d: float
    amount_1w: float
    timestamp: datetime

    @classmethod
    @pipeline(id=1)
    def first_pipeline(
        cls, user: Dataset[User], transaction: Dataset[Transaction]
    ):
        joined = transaction.left_join(user, on=["uid"])
        abroad = joined.filter(
            lambda df: df["country"] != df["payment_country"]
        )
        return abroad.groupby("uid").aggregate(
            [
                Count(window=Window("forever"), into_field="count"),
                Sum(of="amount", window=Window("1d"), into_field="amount_1d"),
                Sum(of="amount", window=Window("1d"), into_field="amount_1w"),
            ]
        )


# /docsnip


# Tests to ensure that there are no run time errors in the snippets
@mock_client
def test_transaction_aggregation_example(client):
    client.sync(datasets=[User, Transaction, UserTransactionsAbroad])
    now = datetime.now()
    dob = now - timedelta(days=365 * 30)
    data = [
        [1, dob, "US", now - timedelta(days=1)],
        [2, dob, "India", now - timedelta(days=1)],
        [3, dob, "China", now - timedelta(days=2)],
    ]

    df = pd.DataFrame(data, columns=["uid", "dob", "country", "signup_time"])

    client.log("User", df)

    data = [
        [1, 100, "US", 1, now],
        [1, 200, "India", 2, now],
        [1, 300, "China", 3, now],
        [2, 100, "Russia", 1, now],
        [2, 200, "India", 2, now],
        [2, 300, "Indonesia", 3, now],
        [3, 100, "US", 1, now],
        [3, 200, "Vietnam", 2, now],
        [3, 300, "Thailand", 3, now],
    ]
    df = pd.DataFrame(
        data,
        columns=[
            "uid",
            "amount",
            "payment_country",
            "merchant_id",
            "timestamp",
        ],
    )
    res = client.log("Transaction", df)
    assert res.status_code == 200, res.json()

    # Do a lookup on UserTransactionsAbroad
    # docsnip dataset_lookup
    uids = pd.Series([1, 2, 3, 4])
    ts = pd.Series([now, now, now, now])
    data, found = UserTransactionsAbroad.lookup(ts, uid=uids)
    # /docsnip
    assert data["uid"].tolist() == [1, 2, 3, 4]
    assert data["count"].tolist() == [2, 2, 3, None]
    assert data["amount_1d"].tolist() == [500, 400, 600, None]
    assert found.to_list() == [True, True, True, False]


@meta(owner="me@fennel.ai")
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


# docsnip transform_pipeline
@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class FraudActivityDataset:
    timestamp: datetime
    user_id: int
    merchant_id: int
    transaction_amount: float

    @pipeline(id=1)
    def create_fraud_dataset(cls, activity: Dataset[Activity]):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df = pd.concat([df_json, df[["user_id", "timestamp"]]], axis=1)
            df["transaction_amount"] = df["transaction_amount"] / 100
            return df[
                [
                    "merchant_id",
                    "transaction_amount",
                    "user_id",
                    "timestamp",
                ]
            ]

        filtered_ds = activity.filter(lambda df: df["action_type"] == "report")
        return filtered_ds.transform(
            extract_info,
            schema={
                "transaction_amount": float,
                "merchant_id": int,
                "user_id": int,
                "timestamp": datetime,
            },
        )


# /docsnip


@mock_client
def test_fraud(client):
    # # Sync the dataset
    client.sync(datasets=[Activity, FraudActivityDataset])
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
    # Only the mock client contains the data parameter to access the data
    # directly for all datasets.
    assert client.data["FraudActivityDataset"].shape == (6, 4)


# docsnip multiple_pipelines
@meta(owner="me@fennel.ai")
@dataset
class AndroidLogins:
    uid: int
    login_time: datetime


@meta(owner="me@fennel.ai")
@dataset
class IOSLogins:
    uid: int
    login_time: datetime


@meta(owner="me@fennel.ai")
@dataset
class LoginStats:
    uid: int = field(key=True)
    platform: str
    num_logins_1d: int
    login_time: datetime

    @pipeline(id=1)
    def android_logins(cls, logins: Dataset[AndroidLogins]) -> Dataset:
        aggregated = logins.groupby(["uid"]).aggregate(
            [
                Count(window=Window("1d"), into_field="num_logins_1d"),
            ]
        )
        return aggregated.transform(
            lambda df: add_platform(df, "android"),
            schema={
                "uid": int,
                "num_logins_1d": int,
                "login_time": datetime,
                "platform": str,
            },
        )

    @pipeline(id=2)
    def ios_logins(cls, logins: Dataset[IOSLogins]) -> Dataset:
        aggregated = logins.groupby(["uid"]).aggregate(
            [
                Count(window=Window("1d"), into_field="num_logins_1d"),
            ]
        )
        return aggregated.transform(
            lambda df: add_platform(df, "ios"),
            schema={
                "uid": int,
                "num_logins_1d": int,
                "login_time": datetime,
                "platform": str,
            },
        )


def add_platform(df: pd.DataFrame, name: str) -> pd.DataFrame:
    df["platform"] = name
    return df


# /docsnip


@mock_client
def test_multiple_pipelines(client):
    client.sync(datasets=[AndroidLogins, IOSLogins, LoginStats])
    now = datetime.now()
    data = [
        [1, now],
        [1, now - timedelta(days=1)],
        [2, now],
        [3, now],
    ]
    df = pd.DataFrame(data, columns=["uid", "login_time"])
    res = client.log("AndroidLogins", df)
    assert res.status_code == 200, res.json()

    data = [
        [1, now],
        [12, now],
        [12, now - timedelta(days=1)],
        [13, now],
    ]
    df = pd.DataFrame(data, columns=["uid", "login_time"])
    res = client.log("IOSLogins", df)
    assert res.status_code == 200, res.json()
    ts = pd.Series([now, now, now, now])
    df, found = LoginStats.lookup(ts, uid=pd.Series([1, 2, 3, 12]))
    assert df.shape == (4, 4)
    assert df["platform"].tolist() == ["ios", "android", "android", "ios"]
    assert found.all()
