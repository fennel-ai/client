from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.test_lib import mock_client


# docsnip user_dataset
@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class UserDataset:
    uid: int = field(key=True)
    dob: datetime
    country: str
    update_time: datetime = field(timestamp=True)


# /docsnip

# This docsnip is not used in the docs, but is used in the tests
# since docs requires not compilable credentials.
from fennel.sources import source, Postgres, Kafka

postgres = Postgres.get(name="postgres")
kafka = Kafka.get(name="kafka")


# docsnip external_data_sources
@meta(owner="data-eng-oncall@fennel.ai")
@source(postgres.table("user", cursor="update_timestamp"), every="1m")
@dataset
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    signup_time: datetime = field(timestamp=True)


@meta(owner="data-eng-oncall@fennel.ai")
@source(kafka.topic("transactions"))
@dataset
class Transaction:
    uid: int
    amount: float
    payment_country: str
    merchant_id: int
    timestamp: datetime = field(timestamp=True)


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
    timestamp: datetime = field(timestamp=True)

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

from datetime import timedelta

# docsnip featureset
from fennel.featuresets import feature, featureset, extractor
from fennel.lib.schema import Series


@featureset
class UserFeature:
    uid: int = feature(id=1)
    country: str = feature(id=2)
    age: float = feature(id=3)
    dob: datetime = feature(id=4)

    @extractor
    def get_age(cls, ts: Series[datetime], uids: Series[uid]) -> Series[age]:
        dobs = User.lookup(ts=ts, uid=uids, fields=["dob"])
        ages = [dob - datetime.now() for dob in dobs]
        return pd.Series(ages)

    @extractor
    def get_country(
        cls, ts: Series[datetime], uids: Series[uid]
    ) -> Series[country]:
        countries, _ = User.lookup(ts=ts, uid=uids, fields=["country"])
        return countries


# /docsnip


# Tests to ensure that there are no run time errors in the snippets
@mock_client
def test_overview(client):
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
