import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from typing import List

from fennel.datasets import pipeline, Dataset
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs
from fennel.test_lib import mock


__owner__ = "data-eng@fennel.ai"


def test_datasets_basic():
    # docsnip datasets
    from fennel.datasets import dataset, field
    from fennel.sources import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    @source(webhook.endpoint("User"))
    @dataset
    class User:
        uid: int = field(key=True)
        dob: datetime
        country: str
        signup_time: datetime = field(timestamp=True)

    @source(webhook.endpoint("Transaction"))
    @dataset
    class Transaction:
        uid: int
        amount: float
        payment_country: str
        merchant_id: int
        timestamp: datetime

    # /docsnip
    return User, Transaction


def test_pipeline_basic():
    User, Transaction = test_datasets_basic()
    # docsnip pipeline
    from fennel.datasets import pipeline, Dataset, dataset, field
    from fennel.lib.aggregate import Count, Sum

    @dataset
    class UserTransactionsAbroad:
        uid: int = field(key=True)
        count: int
        amount_1d: float
        amount_1w: float
        timestamp: datetime

        @classmethod
        @pipeline(version=1)
        @inputs(User, Transaction)
        def first_pipeline(cls, user: Dataset, transaction: Dataset):
            joined = transaction.join(user, how="left", on=["uid"])
            abroad = joined.filter(
                lambda df: df["country"] != df["payment_country"]
            )
            return abroad.groupby("uid").aggregate(
                Count(window="forever", into_field="count"),
                Sum(of="amount", window="1d", into_field="amount_1d"),
                Sum(of="amount", window="1w", into_field="amount_1w"),
            )

    # /docsnip
    return UserTransactionsAbroad


# Tests to ensure that there are no run time errors in the snippets
@mock
def test_transaction_aggregation_example(client):
    User, Transaction = test_datasets_basic()
    UserTransactionsAbroad = test_pipeline_basic()
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
    assert found.to_list() == [True, True, True, False]


@mock
def test_fraud(client):
    from fennel.datasets import dataset, field
    from fennel.sources import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    @source(webhook.endpoint("Activity"))
    @dataset
    class Activity:
        uid: int
        action_type: str
        merchant_id: int
        amount: str = field().meta(description="amount in dollars as str")
        timestamp: datetime

    # docsnip transform_pipeline
    @dataset
    class FraudActivity:
        uid: int
        merchant_id: int
        amount_cents: float
        timestamp: datetime

        @pipeline(version=1)
        @inputs(Activity)
        def create_fraud_dataset(cls, activity: Dataset):
            return (
                activity.filter(lambda df: df["action_type"] == "report")
                .assign(
                    "amount_cents",
                    float,
                    lambda df: df["amount"].astype(float) / 100,
                )
                .drop("action_type", "amount")
            )

    # /docsnip
    # # Sync the dataset
    client.sync(datasets=[Activity, FraudActivity])
    now = datetime.now()
    minute_ago = now - timedelta(minutes=1)
    data = [
        [
            18232,
            "report",
            49,
            "11.91",
            minute_ago,
        ],
        [
            13423,
            "atm_withdrawal",
            99,
            "12.13",
            minute_ago,
        ],
        [
            14325,
            "report",
            99,
            "5",
            minute_ago,
        ],
        [
            18347,
            "atm_withdrawal",
            209,
            "7.45",
            minute_ago,
        ],
        [
            18232,
            "report",
            49,
            "131.58",
            minute_ago,
        ],
    ]
    columns = ["uid", "action_type", "merchant_id", "amount", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    response = client.log("fennel_webhook", "Activity", df)
    assert response.status_code == requests.codes.OK, response.json()
    # Only the mock client contains the data parameter to access the data
    # directly for all datasets.
    assert client.data["FraudActivity"].shape == (3, 4)


@mock
def test_multiple_pipelines(client):
    # docsnip multiple_pipelines
    from fennel.datasets import dataset, field
    from fennel.sources import source, Webhook
    from fennel.lib.aggregate import Count

    webhook = Webhook(name="fennel_webhook")

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
                Count(window="1d", into_field="num_logins_1d"),
            )

    # /docsnip
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
