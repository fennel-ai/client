import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_kafka_source(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    # docsnip basic
    from fennel.sources import source, Kafka
    from fennel.datasets import dataset, field

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

    @source(kafka.topic("user", format="json")) # docsnip-highlight
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip

    client.commit(datasets=[SomeDataset])


@mock
def test_kafka_with_avro(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    os.environ["SCHEMA_REGISTRY_USERNAME"] = "test"
    os.environ["SCHEMA_REGISTRY_PASSWORD"] = "test"
    # docsnip kafka_with_avro
    from fennel.sources import source, Kafka, Avro
    from fennel.datasets import dataset, field

    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",  # could come via os env var too
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )

    avro = Avro(
        registry="confluent",
        url=os.environ["SCHEMA_REGISTRY_URL"],
        username=os.environ["SCHEMA_REGISTRY_USERNAME"],
        password=os.environ["SCHEMA_REGISTRY_PASSWORD"],
    )

    @source(kafka.topic("user", format=avro))
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip

    client.commit(datasets=[SomeDataset])
