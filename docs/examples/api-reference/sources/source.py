import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_source_decorator(client):
    os.environ["KAFKA_USERNAME"] = "test"
    os.environ["KAFKA_PASSWORD"] = "test"
    os.environ["SCHEMA_REGISTRY_URL"] = "http://localhost:8081"
    os.environ["SCHEMA_REGISTRY_USERNAME"] = "test"
    os.environ["SCHEMA_REGISTRY_PASSWORD"] = "test"
    # docsnip source_decorator
    from fennel.sources import source, S3, ref
    from fennel.datasets import dataset, field

    s3 = S3(name="my_s3")  # using IAM role based access

    bucket = s3.bucket("data", path="user/*/date-%Y-%m-%d/*", format="parquet")

    # docsnip-highlight start
    @source(
        bucket,
        every="1h",
        cdc="append",
        disorder="2d",
        since=datetime(2021, 1, 1, 3, 30, 0),  # 3:30 AM on 1st Jan 2021
        until=datetime(2022, 1, 1, 0, 0, 0),  # 12:00 AM on 1st Jan 2022
        preproc={
            "uid": ref("user_id"),  # 'uid' comes from column 'user_id'
            "country": "USA",  # country for every row should become 'USA'
        },
        tier="prod",
    )
    # docsnip-highlight end
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        country: str
        timestamp: datetime

    # /docsnip
    client.commit(datasets=[User])
