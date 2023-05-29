from datetime import datetime

import pytest
from typing import Optional

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.sources import (
    source,
    MySQL,
    S3,
    Snowflake,
    Kafka,
)

# noinspection PyUnresolvedReferences
from fennel.test_lib import *

mysql = MySQL(
    name="mysql",
    host="localhost",
    db_name="test",
    username="root",
    password="root",
)

snowflake = Snowflake(
    name="snowflake_src",
    account="nhb38793.us-west-2.snowflakecomputing.com",
    warehouse="TEST",
    db_name="MOVIELENS",
    src_schema="PUBLIC",
    role="ACCOUNTADMIN",
    username="<username>",
    password="<password>",
)

kafka = Kafka(
    name="kafka_src",
    bootstrap_servers="localhost:9092",
    topic="test_topic",
    group_id="test_group",
    security_protocol="PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="test",
    sasl_plain_password="test",
)


def test_simple_source():
    with pytest.raises(TypeError) as e:

        @source(mysql.table("user"), every="1h")
        @dataset
        class UserInfoDataset:
            user_id: int = field(key=True)
            name: str
            gender: str
            # Users date of birth
            dob: str
            age: int
            account_creation_date: datetime
            country: Optional[str]
            timestamp: datetime = field(timestamp=True)

    assert str(e.value).endswith(
        "table() missing 1 required positional argument: 'cursor'"
    )

    with pytest.raises(TypeError) as e:

        @source(mysql, every="1h")
        @dataset
        class UserInfoDataset2:
            user_id: int = field(key=True)
            name: str
            gender: str
            # Users date of birth
            dob: str
            age: int
            account_creation_date: datetime
            country: Optional[str]
            timestamp: datetime = field(timestamp=True)

    assert (
        str(e.value) == "mysql does not specify required fields table, cursor."
    )

    with pytest.raises(TypeError) as e:

        @source(mysql.table(cursor="xyz"), every="1h")
        @dataset
        class UserInfoDataset3:
            user_id: int = field(key=True)
            name: str
            gender: str
            # Users date of birth
            dob: str
            age: int
            account_creation_date: datetime
            country: Optional[str]
            timestamp: datetime = field(timestamp=True)

    assert str(e.value).endswith(
        "table() missing 1 required positional argument: 'table_name'"
    )


s3 = S3(
    name="ratings_source",
    bucket_name="all_ratings",
    path_prefix="prod/apac/",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
    src_schema={"Name": "string", "Weight": "number", "Age": "integer"},
    delimiter=",",
)


def test_invalid_s3_source():
    with pytest.raises(AttributeError) as e:

        @source(s3.table("user"), every="1h")
        @dataset
        class UserInfoDataset:
            user_id: int = field(key=True)
            name: str
            gender: str
            # Users date of birth
            dob: str
            age: int
            account_creation_date: datetime
            country: Optional[str]
            timestamp: datetime = field(timestamp=True)

        view = InternalTestClient()
        view.add(UserInfoDataset)
        sync_request = view._get_sync_request_proto()
        assert len(sync_request.dataset_info) == 1
        dataset_request = sync_request.dataset_info[0]
        assert len(dataset_request.sources) == 3

    assert str(e.value) == "'S3' object has no attribute 'table'"


def test_multiple_sources():
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @source(kafka.topic("test_topic"))
        @source(mysql.table("users_mysql", cursor="added_on"), every="1h")
        @source(snowflake.table("users_Sf", cursor="added_on"), every="1h")
        @source(
            s3.bucket(
                bucket_name="all_ratings",
                prefix="prod/apac/",
            ),
            every="1h",
            lateness="2d",
        )
        @dataset
        class UserInfoDataset:
            user_id: int = field(key=True)
            name: str
            gender: str
            # Users date of birth
            dob: str
            age: int
            account_creation_date: datetime
            country: Optional[str]
            timestamp: datetime = field(timestamp=True)

    assert (
        str(e.value)
        == "Multiple sources are not supported in dataset `UserInfoDataset`."
    )
