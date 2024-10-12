import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "saiharsha@fennel.ai"


@mock
def test_snowflake_sink(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    os.environ["SNOWFLAKE_USERNAME"] = "some-name"
    os.environ["SNOWFLAKE_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"

    from fennel.connectors import source, Kafka, Snowflake
    from fennel.datasets import dataset, field

    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",  # could come via os env var too
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )

    # docsnip-highlight next-line
    @source(kafka.topic("user", format="json"), disorder="14d", cdc="upsert")
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    from fennel.connectors import source, Kafka
    from fennel.datasets import dataset, field, pipeline, Dataset
    from fennel.lib.params import inputs

    # docsnip-highlight start
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
    # docsnip-highlight end

    # docsnip basic
    from fennel.connectors import sink

    @dataset
    @sink(
        snowflake.table("test_table"),
        every="1d",
        how="incremental",
        renames={"uid": "new_uid"},
    )  # docsnip-highlight
    class SomeDatasetFiltered:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

        @pipeline
        @inputs(SomeDataset)
        def gmail_filtered(cls, dataset: Dataset):
            return dataset.filter(
                lambda row: row["email"].contains("gmail.com")
            )

    # /docsnip

    client.commit(
        message="some commit msg", datasets=[SomeDataset, SomeDatasetFiltered]
    )
