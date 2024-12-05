import sys
from datetime import datetime

from fennel.connectors import Kafka, S3, eval, source, sink
from fennel.datasets import dataset
from fennel.expr import col
from fennel.testing import mock

__owner__ = "owner@example.com"


@mock
def test_stacked_sinks(client):
    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="user",
        sasl_plain_password="password",
    )
    s3 = S3(name="mys3")
    cutoff = datetime(2024, 1, 1, 0, 0, 0)
    bucket = s3.bucket("data", path="orders")

    # docsnip-highlight start
    @source(bucket, disorder="1w", cdc="append", until=cutoff)
    @source(kafka.topic("order"), disorder="1d", cdc="append", since=cutoff)
    # docsnip-highlight end
    @dataset
    class UserLocation:
        uid: int
        city: str
        country: str
        update_time: datetime

    # docsnip stacked
    from fennel.connectors import sink, Kafka, Snowflake
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
    snowflake = Snowflake(
        name="my_snowflake",
        account="VPECCVJ-MUB03765",
        warehouse="TEST",
        db_name=os.environ["DB_NAME"],
        schema="PUBLIC",
        role="ACCOUNTADMIN",
        username=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )

    # docsnip-highlight start
    @sink(
        snowflake.table("test_table"),
        every="1d",
        how="incremental",
        renames={"uid": "new_uid"},
        stacked=True,
    )
    @sink(kafka.topic("user_location"), cdc="debezium")
    # docsnip-highlight end
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
    client.commit(message="some commit msg", datasets=[Order])
