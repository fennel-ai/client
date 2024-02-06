from datetime import datetime

import os
import pandas as pd

from fennel.sources import Webhook 
from fennel.test_lib import mock
from fennel.lib.metadata import meta
from fennel.datasets import dataset, field

__owner__ = "owner@example.com"


# docsnip user_dataset
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
webhook = Webhook(name="fennel_webhook")


# docsnip external_data_sources
@source(postgres.table("user", cursor="update_timestamp"), every="1m")
@dataset
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    signup_time: datetime = field(timestamp=True)


@source(kafka.topic("transactions"))
@dataset
class Transaction:
    uid: int
    amount: float
    payment_country: str
    merchant_id: int
    timestamp: datetime = field(timestamp=True)


# /docsnip

from fennel.datasets import pipeline, Dataset
from fennel.lib.aggregate import Count, Sum
from fennel.lib.schema import inputs


# docsnip pipeline
@dataset
class UserTransactionsAbroad:
    uid: int = field(key=True)
    count: int
    amount_1d: float
    amount_1w: float
    timestamp: datetime = field(timestamp=True)

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

from datetime import timedelta

from fennel.featuresets import feature, featureset, extractor
from fennel.lib.schema import inputs, outputs


# docsnip featureset
@featureset
class UserFeature:
    uid: int = feature(id=1)
    country: str = feature(id=2)
    age: float = feature(id=3)
    dob: datetime = feature(id=4)

    @extractor(depends_on=[User])
    @inputs(uid)
    @outputs(age)
    def get_age(cls, ts: pd.Series, uids: pd.Series):
        dobs, _ = User.lookup(ts=ts, uid=uids, fields=["dob"])
        dobs.fillna(pd.Timestamp("2020-01-01"), inplace=True)
        ages = ts - dobs # note: age is calculated as of `ts`
        return pd.Series(ages)

    @extractor(depends_on=[User])
    @inputs(uid)
    @outputs(country)
    def get_country(cls, ts: pd.Series, uids: pd.Series):
        countries, _ = User.lookup(ts=ts, uid=uids, fields=["country"])
        countries = countries.fillna("unknown")
        return countries


# /docsnip


# Tests to ensure that there are no run time errors in the snippets
@mock
def test_overview(client):
    client.sync(
        datasets=[User, Transaction, UserTransactionsAbroad], tier="dev"
    )
    now = datetime.now()
    dob = now - timedelta(days=365 * 30)
    data = [
        [1, dob, "US", now - timedelta(days=1)],
        [2, dob, "India", now - timedelta(days=1)],
        [3, dob, "China", now - timedelta(days=2)],
    ]

    df = pd.DataFrame(data, columns=["uid", "dob", "country", "signup_time"])

    response = client.log("fennel_webhook", "User", df)
    assert response.status_code == 200, response.json()

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


os.environ["POSTGRES_HOST"] = "some-host"
os.environ["POSTGRES_DB_NAME"] = "some-db-name"
os.environ["POSTGRES_USERNAME"] = "some-username"
os.environ["POSTGRES_PASSWORD"] = "some-password"

# docsnip source
from fennel.sources import source, Postgres
from fennel.datasets import dataset

postgres = Postgres(
    name="my-postgres",
    host=os.environ["POSTGRES_HOST"],
    db_name=os.environ["POSTGRES_DB_NAME"],
    port=5432,
    username=os.environ["POSTGRES_USERNAME"],
    password=os.environ["POSTGRES_PASSWORD"],
)


@source(postgres.table("user", cursor="update_time"), every="1m")
@dataset
class UserLocation:
    uid: int
    city: str
    country: str
    update_time: datetime


# /docsnip


os.environ["FENNEL_SERVER_URL"] = "http://localhost:8080"
os.environ["FENNEL_TOKEN"] = "my-secret-token"

def dummy_function():
    # docsnip client
    from fennel.client import Client

    client = Client(os.environ["FENNEL_SERVER_URL"], token=os.environ["FENNEL_TOKEN"])
    # /docsnip

@mock
def test_overview(client):
    # docsnip sync
    client.sync(
        datasets=[User, Transaction, UserTransactionsAbroad],
        featuresets=[UserFeature],
    )
    # /docsnip
    # docsnip query

    feature_df = client.extract(
        outputs=[
            UserFeature.age,
            UserFeature.country,
        ],
        inputs=[
            UserFeature.uid,
        ],
        input_dataframe=pd.DataFrame({"UserFeature.uid": [1, 3]}),
    )
    # /docsnip

    # docsnip query_historical
    feature_df = client.extract_historical(
        outputs=[
            UserFeature.age,
            UserFeature.country,
        ],
        inputs=[
            UserFeature.uid,
        ],
        input_dataframe=pd.DataFrame({
            "UserFeature.uid": [1, 3],
            "timestamp": [datetime.now(), datetime.now() - timedelta(days=1)],
        }),
        timestamp_column="timestamp",
    )
    # /docsnip