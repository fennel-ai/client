import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "saiharsha@fennel.ai"


@mock
def test_http_sink(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    os.environ["SNOWFLAKE_USERNAME"] = "some-name"
    os.environ["SNOWFLAKE_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"

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

    # docsnip basic
    from fennel.connectors import sink, HTTP, Certificate
    from fennel.integrations.aws import Secret

    # docsnip-highlight start
    aws_secret = Secret(
        arn="arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret-name-I4hSKr",
        role_arn="arn:aws:iam::123456789012:role/secret-access-role",
    )

    http = HTTP(
        name="http",
        host="http://http-echo-server.harsha.svc.cluster.local:8081/",
        healthz="/health",
        ca_cert=Certificate(aws_secret["ca_cert"]),
    )
    # docsnip-highlight end

    @dataset
    # docsnip-highlight start
    @sink(
        http.path(endpoint="/sink", limit=1000, headers={"Foo": "Bar"}),
        cdc="debezium",
        how="incremental",
    )
    # docsnip-highlight end
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
