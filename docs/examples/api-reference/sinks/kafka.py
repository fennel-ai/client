import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_kafka_sink(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"

    from fennel.connectors import source, Kafka
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
    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",  # could come via os env var too
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )
    # docsnip-highlight end

    # docsnip basic
    from fennel.connectors import sink

    @dataset
    @sink(kafka.topic("gmail_filtered"), cdc="debezium")  # docsnip-highlight
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
