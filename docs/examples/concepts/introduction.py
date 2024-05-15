import os
from datetime import datetime, timezone

import pandas as pd

from fennel.datasets import dataset, field
from fennel.testing import mock

__owner__ = "owner@example.com"


def test_user_dataset():
    # docsnip user_dataset
    @dataset
    class UserDataset:
        uid: int = field(key=True)
        dob: datetime
        country: str
        update_time: datetime = field(timestamp=True)

    # /docsnip


@mock
def test_overview(client):
    # This docsnip is not used in the docs, but is used in the tests
    # since docs requires not compilable credentials.

    from fennel.connectors import source, Kafka, Postgres

    postgres = Postgres.get(name="postgres")
    kafka = Kafka.get(name="kafka")

    # docsnip external_data_sources
    user_table = postgres.table("user", cursor="signup_at")

    # docsnip-highlight next-line
    @source(user_table, every="1m", disorder="7d", cdc="upsert")
    @dataset(index=True)
    class User:
        uid: int = field(key=True)
        dob: datetime
        country: str
        signup_at: datetime = field(timestamp=True)

    # docsnip-highlight next-line
    @source(kafka.topic("txn"), disorder="1d", cdc="append")
    @dataset
    class Transaction:
        uid: int
        amount: float
        payment_country: str
        merchant_id: int
        timestamp: datetime = field(timestamp=True)

    # /docsnip

    from fennel.datasets import pipeline, Dataset, Count, Sum
    from fennel.lib import inputs

    # docsnip pipeline
    @dataset(index=True)
    class UserTransactionsAbroad:
        uid: int = field(key=True)
        count: int
        amount_1d: float
        amount_1w: float
        timestamp: datetime = field(timestamp=True)

        # docsnip-highlight start
        @pipeline
        @inputs(User, Transaction)
        def first_pipeline(cls, user: Dataset, transaction: Dataset):
            joined = transaction.join(user, how="left", on=["uid"])
            abroad = joined.filter(
                lambda df: df["country"] != df["payment_country"]
            )
            return abroad.groupby("uid").aggregate(
                count=Count(window="forever"),
                amount_1d=Sum(of="amount", window="1d"),
                amount_1w=Sum(of="amount", window="1w"),
            )

        # docsnip-highlight end

    # /docsnip

    from datetime import timedelta

    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    # docsnip featureset
    @featureset
    class UserFeature:
        uid: int
        country: str
        age: float

        # docsnip-highlight start
        @extractor(deps=[User])
        @inputs("uid")
        @outputs("age")
        def get_age(cls, ts: pd.Series, uids: pd.Series):
            df, _ = User.lookup(ts=ts, uid=uids, fields=["dob"])
            df.fillna(datetime(1970, 1, 1), inplace=True)
            age = (ts - df["dob"]).dt.days / 365  # age is calculated as of `ts`
            return pd.DataFrame(age, columns=["age"])

        # docsnip-highlight end

        @extractor(deps=[User])
        @inputs("uid")
        @outputs("country")
        def get_country(cls, ts: pd.Series, uids: pd.Series):
            countries, _ = User.lookup(ts=ts, uid=uids, fields=["country"])
            countries = countries.fillna("unknown")
            return countries

    # /docsnip

    # docsnip commit
    client.commit(
        message="user: add transaction datasets; first few features",
        datasets=[User, Transaction, UserTransactionsAbroad],
        featuresets=[UserFeature],
    )
    # /docsnip

    now = datetime.now(timezone.utc)
    dob = now - timedelta(days=365 * 30)
    data = [
        [1, dob, "US", now - timedelta(days=1)],
        [2, dob, "India", now - timedelta(days=1)],
        [3, dob, "China", now - timedelta(days=2)],
    ]

    df = pd.DataFrame(data, columns=["uid", "dob", "country", "signup_at"])
    from fennel.testing import log

    log(User, df)

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
    log(Transaction, df)

    # Do a lookup on UserTransactionsAbroad
    # docsnip dataset_lookup
    uids = pd.Series([1, 2, 3, 4])
    ts = pd.Series([now, now, now, now])
    # docsnip-highlight next-line
    data, found = UserTransactionsAbroad.lookup(ts, uid=uids)
    # /docsnip

    assert data["uid"].tolist() == [1, 2, 3, 4]
    assert data["count"].tolist() == [2, 2, 3, pd.NA]
    assert data["amount_1d"].tolist() == [500, 400, 600, pd.NA]
    assert found.to_list() == [True, True, True, False]

    # docsnip query
    feature_df = client.query(
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
    feature_df = client.query_offline(
        outputs=[
            UserFeature.age,
            UserFeature.country,
        ],
        inputs=[
            UserFeature.uid,
        ],
        input_dataframe=pd.DataFrame(
            {
                "UserFeature.uid": [1, 3],
                "timestamp": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc) - timedelta(days=1),
                ],
            }
        ),
        timestamp_column="timestamp",
    )
    # /docsnip
    # just something to use feature_df to remove lint warning
    return feature_df.shape


def test_source_snip():
    os.environ["POSTGRES_HOST"] = "some-host"
    os.environ["POSTGRES_DB_NAME"] = "some-db-name"
    os.environ["POSTGRES_USERNAME"] = "some-username"
    os.environ["POSTGRES_PASSWORD"] = "some-password"

    # docsnip source
    from fennel.connectors import source, Postgres
    from fennel.datasets import dataset

    # docsnip-highlight start
    postgres = Postgres(
        name="my-postgres",
        host=os.environ["POSTGRES_HOST"],
        db_name=os.environ["POSTGRES_DB_NAME"],
        port=5432,
        username=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    table = postgres.table("user", cursor="update_time")

    # docsnip-highlight next-line
    @source(table, disorder="14d", cdc="append", every="1m")
    @dataset
    class UserLocation:
        uid: int
        city: str
        country: str
        update_time: datetime

    # /docsnip


def test_sink_snip():
    os.environ["POSTGRES_HOST"] = "some-host"
    os.environ["POSTGRES_DB_NAME"] = "some-db-name"
    os.environ["POSTGRES_USERNAME"] = "some-username"
    os.environ["POSTGRES_PASSWORD"] = "some-password"
    os.environ["KAFKA_HOST"] = "some-host"
    os.environ["KAFKA_USERNAME"] = "some-username"
    os.environ["KAFKA_PASSWORD"] = "some-password"

    from fennel.connectors import source, Postgres
    from fennel.datasets import dataset

    postgres = Postgres(
        name="my-postgres",
        host=os.environ["POSTGRES_HOST"],
        db_name=os.environ["POSTGRES_DB_NAME"],
        port=5432,
        username=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    table = postgres.table("user", cursor="update_time")

    @source(table, disorder="14d", cdc="append", every="1m")
    @dataset
    class UserLocation:
        uid: int
        city: str
        country: str
        update_time: datetime

    # docsnip sink_main
    from fennel.connectors import sink, Kafka
    from fennel.datasets import dataset, pipeline, Dataset
    from fennel.lib.params import inputs

    kafka = Kafka(
        name="kafka_src",
        bootstrap_servers=os.environ["KAFKA_HOST"],
        security_protocol="PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )

    # docsnip-highlight next-line
    @sink(kafka.topic("user_location"), cdc="debezium")
    @dataset
    class UserLocationFiltered:
        uid: int
        city: str
        country: str
        update_time: datetime

        @pipeline
        @inputs(UserLocation)
        def user_location_count(cls, dataset: Dataset):
            return dataset.filter(lambda row: row["country"] != "United States")

    # /docsnip


def test_bounded_idleness_snip():
    os.environ["POSTGRES_HOST"] = "some-host"
    os.environ["POSTGRES_DB_NAME"] = "some-db-name"
    os.environ["POSTGRES_USERNAME"] = "some-username"
    os.environ["POSTGRES_PASSWORD"] = "some-password"

    # docsnip bounded_idleness
    from fennel.connectors import source, Postgres, Webhook
    from fennel.datasets import dataset, Dataset
    from fennel import pipeline
    from fennel.lib import inputs

    postgres = Postgres(
        name="my-postgres",
        host=os.environ["POSTGRES_HOST"],
        db_name=os.environ["POSTGRES_DB_NAME"],
        port=5432,
        username=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    webhook = Webhook(name="fennel_webhook")

    # Below is a batch source which is bounded. After initial ingestion, this source is marked as closed
    # due to no data flow for idleness time period.
    @source(
        postgres.table("User", "timestamp"),
        disorder="1d",
        cdc="append",
        bounded=True,
        idleness="1h",
    )
    @dataset
    class OldUsers:
        uid: int
        dob: datetime
        country: str
        signup_time: datetime = field(timestamp=True)

    # Below is a stream source which is unbounded
    @source(webhook.endpoint("User"), disorder="1d", cdc="append")
    @dataset
    class NewUsers:
        uid: int
        dob: datetime
        country: str
        signup_time: datetime = field(timestamp=True)

    @dataset
    class Users:
        uid: int
        dob: datetime
        country: str
        signup_time: datetime = field(timestamp=True)

        @pipeline
        @inputs(OldUsers, NewUsers)
        def club_data(cls, old_users: Dataset, new_users: Dataset):
            ds_batch = old_users.filter(
                lambda df: df["signup_time"] < datetime(2024, 2, 20)
            )
            ds_stream = new_users.filter(
                lambda df: df["signup_time"] >= datetime(2024, 2, 20)
            )
            return ds_batch + ds_stream

    # /docsnip


def dummy_function():
    os.environ["FENNEL_SERVER_URL"] = "http://localhost:8080"
    os.environ["FENNEL_TOKEN"] = "my-secret-token"
    # docsnip client
    from fennel.client import Client

    client = Client(
        os.environ["FENNEL_SERVER_URL"], token=os.environ["FENNEL_TOKEN"]
    )
    # /docsnip
    # just do something with the client to avoid unused variable warning
    return client


@mock
def test_branches(client):
    from fennel.datasets import dataset, field
    from fennel.featuresets import feature as F, featureset
    from fennel.connectors import Webhook, source

    webhook = Webhook(name="some_webhook")

    @source(webhook.endpoint("endpoint1"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class SomeDataset:
        uid: int = field(key=True)
        dob: datetime
        country: str
        update_time: datetime = field(timestamp=True)

    @featureset
    class SomeFeatureset:
        uid: int
        country: str = F(SomeDataset.country, default="unknown")

    # docsnip branches
    client.init_branch("dev")
    client.commit(
        message="some module: some git like commit message",
        datasets=[SomeDataset],
        featuresets=[SomeFeatureset],
    )

    client.query(
        outputs=[SomeFeatureset.country],
        inputs=[SomeFeatureset.uid],
        input_dataframe=pd.DataFrame({"SomeFeatureset.uid": [1, 2]}),
    )
    # /docsnip
