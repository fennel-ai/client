import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_kafka_source(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    # docsnip basic
    from fennel.connectors import source, Kafka
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

    # docsnip-highlight start
    @source(kafka.topic("user", format="json"), disorder="14d", cdc="upsert")
    # docsnip-highlight end
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip

    client.commit(message="some commit msg", datasets=[SomeDataset])


@mock
def test_kafka_source_with_secret(client):
    # docsnip secret
    from fennel.connectors import source, Kafka, Avro
    from fennel.datasets import dataset, field
    from fennel.integrations.aws import Secret

    # docsnip-highlight start
    aws_secret = Secret(
        arn="arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret-name-I4hSKr",
        role_arn="arn:aws:iam::123456789012:role/secret-access-role",
    )

    # secret with above arn has content like below
    # {
    #     "kafka": {
    #         "username": "test",
    #         "password": "test"
    #     },
    #     "schema_registry": {
    #         "username": "test",
    #         "password": "test"
    #     }
    # }
    #

    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",  # could come via os env var too
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=aws_secret["kafka"]["username"],
        sasl_plain_password=aws_secret["kafka"]["password"],
    )
    avro = Avro(
        registry="confluent",
        url=os.environ["SCHEMA_REGISTRY_URL"],
        username=aws_secret["schema_registry"]["username"],
        password=aws_secret["schema_registry"]["password"],
    )
    # docsnip-highlight end

    # docsnip-highlight start
    @source(kafka.topic("user", format=avro), disorder="14d", cdc="upsert")
    # docsnip-highlight end
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip

    client.commit(message="some commit msg", datasets=[SomeDataset])


@mock
def test_kafka_with_avro(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    os.environ["SCHEMA_REGISTRY_USERNAME"] = "test"
    os.environ["SCHEMA_REGISTRY_PASSWORD"] = "test"
    # docsnip kafka_with_avro
    from fennel.connectors import source, Kafka, Avro
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

    @source(kafka.topic("user", format=avro), disorder="14d", cdc="upsert")
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip

    client.commit(message="msg", datasets=[SomeDataset])


@mock
def test_kafka_with_protobuf(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    os.environ["SCHEMA_REGISTRY_USERNAME"] = "test"
    os.environ["SCHEMA_REGISTRY_PASSWORD"] = "test"
    # docsnip kafka_with_protobuf
    from fennel.connectors import source, Kafka, Protobuf
    from fennel.datasets import dataset, field

    kafka = Kafka(
        name="my_kafka",
        bootstrap_servers="localhost:9092",  # could come via os env var too
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )

    # docsnip-highlight start
    protobuf = Protobuf(
        registry="confluent",
        url=os.environ["SCHEMA_REGISTRY_URL"],
        username=os.environ["SCHEMA_REGISTRY_USERNAME"],
        password=os.environ["SCHEMA_REGISTRY_PASSWORD"],
    )
    # docsnip-highlight end

    # docsnip-highlight start
    @source(kafka.topic("user", format=protobuf), disorder="14d", cdc="upsert")
    # docsnip-highlight end
    @dataset
    class SomeDataset:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip

    client.commit(message="msg", datasets=[SomeDataset])
