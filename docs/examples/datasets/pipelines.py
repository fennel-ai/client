import json
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import requests

from fennel.datasets import dataset, field
from fennel.datasets import pipeline, Dataset
from fennel.lib.aggregate import Count, Sum, LastK
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs
from fennel.lib.window import Window
from fennel.sources import source, Webhook
from fennel.test_lib import mock

webhook = Webhook(name="fennel_webhook")


# docsnip data_sets
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("User"))
@dataset
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    signup_time: datetime = field(timestamp=True)


@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("Transaction"))
@dataset
class Transaction:
    uid: int
    amount: float
    payment_country: str
    merchant_id: int
    timestamp: datetime


# /docsnip


# docsnip pipeline
@meta(owner="ahmed@fennel.ai")
@dataset
class UserTransactionsAbroad:
    uid: int = field(key=True)
    count: int
    amount_1d: float
    amount_1w: float
    recent_merchant_ids: List[int]
    timestamp: datetime

    @classmethod
    @pipeline(version=1)
    @inputs(User, Transaction)
    def first_pipeline(cls, user: Dataset, transaction: Dataset):
        joined = transaction.join(user, how='left', on=["uid"])
        abroad = joined.filter(
            lambda df: df["country"] != df["payment_country"]
        )
        return abroad.groupby("uid").aggregate(
            [
                Count(window=Window("forever"), into_field="count"),
                Sum(of="amount", window=Window("1d"), into_field="amount_1d"),
                Sum(of="amount", window=Window("1d"), into_field="amount_1w"),
                LastK(
                    of="merchant_id",
                    window=Window("1d"),
                    into_field="recent_merchant_ids",
                    limit=5,
                    dedup=True,
                ),
            ]
        )


# /docsnip


# Tests to ensure that there are no run time errors in the snippets
@mock
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

    client.log("fennel_webhook", "User", df)

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
    res = client.log("fennel_webhook", "Transaction", df)
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
    assert data["recent_merchant_ids"].tolist() == [
        [2, 3],
        [1, 3],
        [1, 2, 3],
        None,
    ]
    assert found.to_list() == [True, True, True, False]


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("Activity"))
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

    @pipeline(version=1)
    @inputs(Activity)
    def create_fraud_dataset(cls, activity: Dataset):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df = pd.concat([df_json, df[["user_id", "timestamp"]]], axis=1)
            df["transaction_amount"] = df["transaction_amount"] / 100
            return df[
                ["merchant_id", "transaction_amount", "user_id", "timestamp"]
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


@mock
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
    response = client.log("fennel_webhook", "Activity", df)
    assert response.status_code == requests.codes.OK, response.json()
    # Only the mock client contains the data parameter to access the data
    # directly for all datasets.
    assert client.data["FraudActivityDataset"].shape == (6, 4)


# docsnip multiple_pipelines
@meta(owner="me@fennel.ai")
@source(webhook.endpoint("AndroidLogins"))
@dataset
class AndroidLogins:
    uid: int
    login_time: datetime


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("IOSLogins"))
@dataset
class IOSLogins:
    uid: int
    login_time: datetime


def add_platform(df: pd.DataFrame, name: str) -> pd.DataFrame:
    df["platform"] = name
    return df


@meta(owner="me@fennel.ai")
@dataset
class LoginStats:
    uid: int = field(key=True)
    platform: str = field(key=True)
    num_logins_1d: int
    login_time: datetime

    @pipeline(version=1, active=True)
    @inputs(AndroidLogins, IOSLogins)
    @includes(add_platform)
    def android_logins(cls, android_logins: Dataset, ios_logins: Dataset):
        with_android_platform = android_logins.transform(
            lambda df: add_platform(df, "android"),
            schema={
                "uid": int,
                "login_time": datetime,
                "platform": str,
            },
        )
        with_ios_platform = ios_logins.transform(
            lambda df: add_platform(df, "ios"),
            schema={
                "uid": int,
                "login_time": datetime,
                "platform": str,
            },
        )
        union = with_ios_platform + with_android_platform
        return union.groupby(["uid", "platform"]).aggregate(
            [
                Count(window=Window("1d"), into_field="num_logins_1d"),
            ]
        )


# /docsnip


@mock
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
    res = client.log("fennel_webhook", "AndroidLogins", df)
    assert res.status_code == 200, res.json()

    data = [
        [1, now],
        [12, now],
        [12, now - timedelta(days=1)],
        [13, now],
    ]
    df = pd.DataFrame(data, columns=["uid", "login_time"])
    res = client.log("fennel_webhook", "IOSLogins", df)
    assert res.status_code == 200, res.json()
    ts = pd.Series([now, now, now, now])
    df, found = LoginStats.lookup(
        ts,
        uid=pd.Series([1, 2, 3, 12]),
        platform=pd.Series(["ios", "android", "android", "ios"]),
    )
    assert df.shape == (4, 4)
    assert found.sum() == 4
