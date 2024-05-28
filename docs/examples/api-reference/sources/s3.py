import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_s3_source_prefix(client):
    os.environ["AWS_ACCESS_KEY_ID"] = "some-access-key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "some-secret"
    # docsnip s3_prefix
    from fennel.connectors import source, S3
    from fennel.datasets import dataset, field

    # docsnip-highlight start
    s3 = S3(
        name="mys3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
    # docsnip-highlight end

    # docsnip-highlight start
    @source(
        s3.bucket("datalake", prefix="user"),
        every="1h",
        disorder="14d",
        cdc="upsert",
    )
    # docsnip-highlight end
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[User])


@mock
def test_s3_delta(client):
    os.environ["AWS_ACCESS_KEY_ID"] = "some-access-key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "some-secret"
    # docsnip s3_delta
    from fennel.connectors import source, S3
    from fennel.datasets import dataset, field

    s3 = S3(
        name="mys3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # docsnip-highlight start
    @source(
        s3.bucket("data", prefix="user", format="delta"),
        every="1h",
        disorder="14d",
        cdc="native",
    )
    # docsnip-highlight end
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip
    client.commit(message="msg", datasets=[User])


@mock
def test_s3_hudi(client):
    os.environ["AWS_ACCESS_KEY_ID"] = "some-access-key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "some-secret"
    # docsnip s3_hudi
    from fennel.connectors import source, S3
    from fennel.datasets import dataset, field

    s3 = S3(
        name="mys3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # docsnip-highlight start
    @source(
        s3.bucket("data", prefix="user", format="hudi"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    # docsnip-highlight end
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip
    client.commit(message="msg", datasets=[User])


@mock
def test_s3_source_path(client):
    os.environ["AWS_ACCESS_KEY_ID"] = "some-access-key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "some-secret"
    # docsnip s3_path
    from fennel.connectors import source, S3
    from fennel.datasets import dataset, field

    # docsnip-highlight start
    s3 = S3(
        name="my_s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    bucket = s3.bucket(
        "data", path="user/*/date-%Y-%m-%d/*", format="parquet", spread="2d"
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    @source(bucket, disorder="14d", cdc="upsert", every="1h")
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    # /docsnip
    client.commit(message="msg", datasets=[User])
