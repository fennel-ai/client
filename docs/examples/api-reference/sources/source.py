import os
import sys
from datetime import datetime

from fennel.expr import col
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
    import pandas as pd
    from fennel.connectors import source, S3, ref, eval
    from fennel.datasets import dataset, field
    from fennel.connectors.connectors import Sample

    s3 = S3(name="my_s3")  # using IAM role based access

    bucket = s3.bucket("data", path="user/*/date-%Y-%m-%d/*", format="parquet")

    if sys.version_info >= (3, 10):
        # docsnip-highlight start
        @source(
            bucket,
            every="1h",
            cdc="upsert",
            disorder="2d",
            since=datetime(2021, 1, 1, 3, 30, 0),  # 3:30 AM on 1st Jan 2021
            until=datetime(2022, 1, 1, 0, 0, 0),  # 12:00 AM on 1st Jan 2022
            preproc={
                "uid": ref("user_id"),  # 'uid' comes from column 'user_id'
                "country": "USA",  # country for every row should become 'USA'
                "age": eval(
                    lambda x: pd.to_numeric(x["age"]).astype(int),
                    schema={"age": str},
                ),  # converting age dtype to int
            },
            where=eval(col("age") >= 18, schema={"age": int}),
            env="prod",
            sample=Sample(0.2, using=["email"]),
            bounded=True,
            idleness="1h",
        )
        # docsnip-highlight end
        @dataset
        class User:
            uid: int = field(key=True)
            email: str
            country: str
            age: int
            timestamp: datetime

        # /docsnip
        client.commit(message="some commit msg", datasets=[User])
