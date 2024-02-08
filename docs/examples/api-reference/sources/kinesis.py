from datetime import datetime
import os

from fennel.test_lib import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_kinesis_init_position(client):
    os.environ["KINESIS_ROLE_ARN"] = "some-role-arn"
    os.environ["KINESIS_ORDERS_STREAM_ARN"] = "some-stream-arn"
    # docsnip kinesis_at_timestamp
    from fennel.sources import source, Kinesis
    from fennel.datasets import dataset, field

    kinesis = Kinesis(
        name="my_kinesis",
        role_arn=os.environ["KINESIS_ROLE_ARN"],
    )

    stream = kinesis.stream(
        stream_arn=os.environ["KINESIS_ORDERS_STREAM_ARN"],
        init_position=datetime(2023, 1, 5),  # Start ingesting from Jan 5, 2023
        format="json",
    )

    @source(stream)
    @dataset
    class Orders:
        uid: int
        order_id: str
        amount: float
        timestamp: datetime

    # /docsnip
    client.sync(datasets=[Orders])


@mock
def test_kinesis_latest(client):
    os.environ["KINESIS_ROLE_ARN"] = "some-role-arn"
    os.environ["KINESIS_ORDERS_STREAM_ARN"] = "some-stream-arn"
    # docsnip kinesis_latest
    from fennel.sources import source, Kinesis
    from fennel.datasets import dataset, field

    kinesis = Kinesis(
        name="my_kinesis",
        role_arn=os.environ["KINESIS_ROLE_ARN"],
    )

    stream = kinesis.stream(
        stream_arn=os.environ["KINESIS_ORDERS_STREAM_ARN"],
        init_position="latest",
        format="json",
    )

    @source(stream)
    @dataset
    class Orders:
        uid: int
        order_id: str
        amount: float
        timestamp: datetime

    # /docsnip
    client.sync(datasets=[Orders])