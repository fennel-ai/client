from datetime import datetime

from fennel.connectors import Kafka, S3, source
from fennel.datasets import dataset
from fennel.testing import mock

__owner__ = "owner@example.com"


@mock
def test_stacked_sources(client):
    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="user",
        sasl_plain_password="password",
    )
    s3 = S3(name="mys3")
    # docsnip stacked
    cutoff = datetime(2024, 1, 1, 0, 0, 0)
    bucket = s3.bucket("data", path="orders")

    # docsnip-highlight start
    @source(bucket, disorder="1w", cdc="append", until=cutoff)
    @source(kafka.topic("order"), disorder="1d", cdc="append", since=cutoff)
    # docsnip-highlight end
    @dataset
    class Order:
        uid: int
        skuid: int
        at: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[Order])


@mock
def test_stacked_sources_in_env(client):
    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="user",
        sasl_plain_password="password",
    )
    s3 = S3(name="mys3")

    # docsnip selector
    # docsnip-highlight start
    @source(s3.bucket("data", path="*"), disorder="1w", cdc="append", env="dev")
    @source(kafka.topic("order"), disorder="1d", cdc="append", env="prod")
    # docsnip-highlight end
    @dataset
    class Order:
        uid: int
        skuid: int
        at: datetime

    client.commit(
        message="some commit msg",
        datasets=[Order],
        # docsnip-highlight next-line
        env="prod",  # use only the sources with env="prod"
    )
    # /docsnip


@mock
def test_filter_preproc_source(client):
    s3 = S3(name="mys3")

    # docsnip where
    # docsnip-highlight start
    @source(
        s3.bucket("data", path="*"),
        disorder="1w",
        cdc="append",
        where=lambda x: x["skuid"] >= 1000,
    )
    # docsnip-highlight end
    @dataset
    class Order:
        uid: int
        skuid: int
        at: datetime

    client.commit(
        message="some commit msg",
        datasets=[Order],
    )
    # /docsnip
