from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from fennel.datasets import pipeline, Dataset
from fennel.lib import includes, meta, inputs
from fennel.testing import mock

__owner__ = "data-eng@fennel.ai"


def test_datasets_basic():
    # docsnip datasets
    from fennel.datasets import dataset, field
    from fennel.connectors import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class User:
        uid: int = field(key=True)
        dob: datetime
        country: str
        signup_time: datetime = field(timestamp=True)

    @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
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
    from fennel.datasets import Count, Sum
    from fennel.dtypes import Continuous

    @dataset(index=True)
    class UserTransactionsAbroad:
        uid: int = field(key=True)
        count: int
        amount_1d: float
        amount_1w: float
        timestamp: datetime

        # docsnip-highlight start
        @pipeline
        @inputs(User, Transaction)
        def first_pipeline(cls, user: Dataset, transaction: Dataset):
            joined = transaction.join(user, how="left", on=["uid"])
            abroad = joined.filter(
                lambda df: df["country"] != df["payment_country"]
            )
            return abroad.groupby("uid").aggregate(
                count=Count(window=Continuous("forever")),
                amount_1d=Sum(of="amount", window=Continuous("1d")),
                amount_1w=Sum(of="amount", window=Continuous("1w")),
            )

        # docsnip-highlight end

    # /docsnip
    return UserTransactionsAbroad


# Tests to ensure that there are no run time errors in the snippets
@mock
def test_transaction_aggregation_example(client):
    User, Transaction = test_datasets_basic()
    UserTransactionsAbroad = test_pipeline_basic()
    client.commit(
        message="msg", datasets=[User, Transaction, UserTransactionsAbroad]
    )
    now = datetime.now(timezone.utc)
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
    assert data["count"].tolist() == [2, 2, 3, pd.NA]
    assert data["amount_1d"].tolist() == [500, 400, 600, pd.NA]
    assert found.to_list() == [True, True, True, False]


@mock
def test_fraud(client):
    from fennel.datasets import dataset, field
    from fennel.connectors import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    @source(webhook.endpoint("Activity"), disorder="14d", cdc="append")
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

        @pipeline
        @inputs(Activity)
        def create_fraud_dataset(cls, activity: Dataset):
            return (
                # docsnip-highlight next-line
                activity.filter(lambda df: df["action_type"] == "report")
                .assign(
                    "amount_cents",
                    float,
                    # docsnip-highlight next-line
                    lambda df: df["amount"].astype(float) / 100,
                )
                .drop("action_type", "amount")
            )

    # /docsnip
    # docsnip transform_pipeline_expr
    # docsnip-highlight next-line
    from fennel.expr import col

    @dataset
    class FraudActivity:
        uid: int
        merchant_id: int
        amount_cents: float
        timestamp: datetime

        @pipeline
        @inputs(Activity)
        def create_fraud_dataset(cls, activity: Dataset):
            return (
                activity
                    # docsnip-highlight start
                    .filter(col("action_type") == "report")
                    .assign(amount_cents=(col("amount") / 100.0).astype(float))
                    # docsnip-highlight end
                    .drop("action_type", "amount")
            )

    # /docsnip
    # # Sync the dataset
    client.commit(message="msg", datasets=[Activity, FraudActivity])
    now = datetime.now(timezone.utc)
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
    assert client.get_dataset_df("FraudActivity").shape == (3, 4)


@mock
def test_multiple_pipelines(client):
    # docsnip multiple_pipelines
    from fennel.datasets import dataset, field, Count
    from fennel.dtypes import Continuous
    from fennel.connectors import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    @meta(owner="me@fennel.ai")
    @source(webhook.endpoint("AndroidLogins"), disorder="14d", cdc="append")
    @dataset
    class AndroidLogins:
        uid: int
        login_time: datetime

    @meta(owner="me@fennel.ai")
    @source(webhook.endpoint("IOSLogins"), disorder="14d", cdc="append")
    @dataset
    class IOSLogins:
        uid: int
        login_time: datetime

    def add_platform(df: pd.DataFrame, name: str) -> pd.DataFrame:
        df["platform"] = name
        return df

    @meta(owner="me@fennel.ai")
    @dataset(index=True)
    class LoginStats:
        uid: int = field(key=True)
        platform: str = field(key=True)
        num_logins_1d: int
        login_time: datetime

        @pipeline
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
                num_logins_1d=Count(window=Continuous("1d")),
            )

    # /docsnip
    client.commit(
        message="msg", datasets=[AndroidLogins, IOSLogins, LoginStats]
    )
    now = datetime.now(timezone.utc)
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
