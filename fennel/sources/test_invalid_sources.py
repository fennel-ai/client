from datetime import datetime
from typing import Optional

import pytest

from fennel.datasets import dataset, field
from fennel.lib import meta
from fennel.sources import (
    source,
    MySQL,
    S3,
    Snowflake,
    Kafka,
    Kinesis,
    at_timestamp,
    BigQuery,
    S3Connector,
)

# noinspection PyUnresolvedReferences
from fennel.testing import *

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
    schema="PUBLIC",
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

kinesis = Kinesis(
    name="kinesis_src",
    role_arn="arn:aws:iam::123456789012:role/test-role",
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

        @source(mysql, disorder="14d", cdc="append", every="1h")
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
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
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


def test_invalid_kinesis_source():
    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @source(
            kinesis.stream("test_stream", format="json", init_position="today")
        )
        @dataset
        class UserInfoDatasetKinesis1:
            user_id: int = field(key=True)
            name: str
            gender: str
            timestamp: datetime = field(timestamp=True)

    assert str(e.value) == "Invalid isoformat string: 'today'"

    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @source(
            kinesis.stream("test_stream", format="json", init_position=None)
        )
        @dataset
        class UserInfoDatasetKinesis2:
            user_id: int = field(key=True)
            name: str
            gender: str
            timestamp: datetime = field(timestamp=True)

    assert (
        str(e.value)
        == "Kinesis init_position must be 'latest', 'trim_horizon' or a timestamp. Invalid timestamp type <class 'NoneType'>"
    )

    with pytest.raises(AttributeError) as e:

        @meta(owner="test@test.com")
        @source(
            kinesis.stream(
                "test_stream",
                at_timestamp(datetime.now()),
                format="csv",
            )
        )
        @dataset
        class UserInfoDatasetKinesis3:
            user_id: int = field(key=True)
            name: str
            gender: str
            timestamp: datetime = field(timestamp=True)

    assert str(e.value) == "Kinesis format must be json"

    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @source(
            kinesis.stream(
                "test_stream",
                "latest(2024-01-01T00:00:00Z)",
                format="csv",
            )
        )
        @dataset
        class UserInfoDatasetKinesis:
            user_id: int = field(key=True)
            name: str
            gender: str
            timestamp: datetime = field(timestamp=True)

    assert (
        str(e.value)
        == "Invalid isoformat string: 'latest(2024-01-01T00:00:00Z)'"
    )


@mock
def test_multiple_sources(client):
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @source(kafka.topic("test_topic"), disorder="14d", cdc="append")
        @source(
            mysql.table("users_mysql", cursor="added_on"),
            disorder="14d",
            cdc="append",
            every="1h",
        )
        @source(
            snowflake.table("users_Sf", cursor="added_on"),
            disorder="14d",
            cdc="append",
            every="1h",
        )
        @source(
            s3.bucket(
                bucket_name="all_ratings",
                prefix="prod/apac/",
            ),
            every="1h",
            cdc="append",
            disorder="2d",
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

        client.commit(message="msg", datasets=[UserInfoDataset], featuresets=[])

    assert (
        str(e.value)
        == "Dataset `UserInfoDataset` has more than one source defined, found 4 sources."
    )


def test_invalid_bigquery_credential():
    with pytest.raises(ValueError) as e:
        BigQuery(
            name="bigquery",
            project_id="project",
            dataset_id="dataset",
            credentials_json="bad_json",
        )

    assert "can't deserialize json" in str(e.value)


def test_invalid_kaffa_security_protocol():
    with pytest.raises(ValueError):
        Kafka(
            name="kafka",
            bootstrap_servers="localhost:9092",
            security_protocol="Wrong Protocol",
        )


bigquery = BigQuery(
    name="bq_movie_tags",
    project_id="gold-cocoa-356105",
    dataset_id="movie_tags",
    credentials_json="{}",
)


def test_invalid_starting_from():
    with pytest.raises(Exception) as e:

        @source(
            s3.bucket(bucket_name="bucket", prefix="prefix"),
            every="1h",
            disorder="14d",
            cdc="append",
            since="2020-01-01T00:00:00Z",
        )
        @meta(owner="aditya@fennel.ai")
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

    assert "'since' must be of type datetime - got <class 'str'>" == str(
        e.value
    )


def test_invalid_until():
    with pytest.raises(Exception) as e:

        @source(
            s3.bucket(bucket_name="bucket", prefix="prefix"),
            every="1h",
            disorder="14d",
            cdc="append",
            until="2020-01-01T00:00:00Z",
        )
        @meta(owner="zaki@fennel.ai")
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

    assert "'until' must be of type datetime - got <class 'str'>" == str(
        e.value
    )

    with pytest.raises(ValueError) as e:

        @source(
            s3.bucket(bucket_name="bucket", prefix="prefix"),
            every="1h",
            disorder="14d",
            cdc="append",
            since=datetime(2024, 1, 1),
            until=datetime(2023, 12, 1),
        )
        @meta(owner="zaki@fennel.ai")
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

    assert ("must be earlier than 'until'") in str(e.value)


def test_invalid_s3_format():
    with pytest.raises(Exception) as e:
        s3.bucket(bucket_name="bucket", prefix="prefix", format="py")
    assert "format must be either" in str(e.value)

    with pytest.raises(Exception) as e:
        s3.bucket(
            bucket_name="bucket", prefix="prefix", format="csv", delimiter="  "
        )
    assert "delimiter must be one of" in str(e.value)


def test_invalid_s3_path():
    # Exactly one of path and prefix are allowed
    with pytest.raises(AttributeError) as e:
        s3.bucket(
            bucket_name="bucket", prefix="prefix", path="prefix/suffix/*.json"
        )
    assert "path and prefix cannot be specified together" == str(e.value)

    with pytest.raises(AttributeError) as e:
        s3.bucket(bucket_name="bucket")
    assert "either path or prefix must be specified" == str(e.value)

    invalid_path_cases = [
        ("foo*/bar*/", "* wildcard must be a complete path part"),
        ("foo/bar*", "of the form *.file-extension"),
        ("foo/*-file.csv", "of the form *.file-extension"),
        ("foo$/bar1/*", "alphanumeric characters, hyphens,"),
        ("date-%q/*", "Invalid datetime"),
        ("//foo/bar", "alphanumeric characters, hyphens,"),
        ("*year=%Y/*", "* wildcard must be a complete path part"),
        ("year=%*/*", "* wildcard must be a complete path part"),
    ]

    for path, error_str in invalid_path_cases:
        with pytest.raises(ValueError) as e:
            S3Connector.parse_path(path)
        assert "Invalid path part" in str(e.value)
        assert error_str in str(e.value)


def test_invalid_spread():
    # spread must be used with path
    with pytest.raises(AttributeError) as e:
        s3.bucket(bucket_name="bucket", prefix="prefix", spread="7d")
    assert "path must be specified to use spread" == str(e.value)

    # spread must be a valid duration
    with pytest.raises(ValueError) as e:
        s3.bucket(
            bucket_name="bucket", path="prefix/foo/yymmdd=%Y%m%d/*", spread="k"
        )
    assert "Spread k is an invalid duration" in str(e.value)

    with pytest.raises(ValueError) as e:
        s3.bucket(
            bucket_name="bucket", path="prefix/foo/yymmdd=%Y%m%d/*", spread="d7"
        )
    assert "Spread d7 is an invalid duration" in str(e.value)


def test_invalid_pre_proc():
    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
        ),
        every="1h",
        disorder="14d",
        cdc="append",
        # column doesn't exist
        preproc={
            "age": 10,
        },
    )
    @meta(owner="test@test.com", tags=["test", "yolo"])
    @dataset
    class UserInfoDataset:
        user_id: int = field(key=True)
        timestamp: datetime = field(timestamp=True)

    with pytest.raises(ValueError):
        view = InternalTestClient()
        view.add(UserInfoDataset)
        view._get_sync_request_proto()

    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
        ),
        every="1h",
        disorder="14d",
        cdc="append",
        # data type is wrong
        preproc={
            "timestamp": 10,
        },
    )
    @meta(owner="test@test.com", tags=["test", "yolo"])
    @dataset
    class UserInfoDataset2:
        user_id: int = field(key=True)
        timestamp: datetime = field(timestamp=True)

    with pytest.raises(ValueError):
        view = InternalTestClient()
        view.add(UserInfoDataset2)
        view._get_sync_request_proto()


def test_invalid_bounded_and_idleness():
    # No idleness for bounded source
    with pytest.raises(AttributeError) as e:
        source(
            s3.bucket(
                bucket_name="all_ratings",
                prefix="prod/apac/",
            ),
            every="1h",
            bounded=True,
            disorder="14d",
            cdc="append",
        )

    assert (
        "idleness parameter should always be passed when bounded is set as True"
        == str(e.value)
    )

    # Idleness for unbounded source
    with pytest.raises(AttributeError) as e:
        source(
            s3.bucket(
                bucket_name="all_ratings",
                prefix="prod/apac/",
            ),
            every="1h",
            idleness="1h",
            disorder="14d",
            cdc="append",
        )

    assert (
        "idleness parameter should not be passed when bounded is set as False"
        == str(e.value)
    )
