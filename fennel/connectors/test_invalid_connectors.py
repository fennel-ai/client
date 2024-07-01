from datetime import datetime, timezone
from typing import Optional

from fennel.connectors.connectors import Protobuf, ref
import pytest

from fennel.connectors import (
    source,
    Mongo,
    sink,
    MySQL,
    S3,
    Snowflake,
    Redshift,
    Kafka,
    Kinesis,
    at_timestamp,
    BigQuery,
    S3Connector,
)
from fennel.datasets import dataset, field
from fennel.lib import meta

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

bigquery = BigQuery(
    name="bigquery_src",
    project_id="my_project",
    dataset_id="my_dataset",
    service_account_key={
        "type": "service_account",
        "project_id": "fake-project-356105",
        "client_email": "randomstring@fake-project-356105.iam.gserviceaccount.com",
        "client_id": "103688493243243272951",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    },
)

redshift = Redshift(
    name="redshift_src",
    s3_access_role_arn="arn:aws:iam::123:role/Redshift",
    db_name="test",
    host="test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
    schema="public",
)

mongo = Mongo(
    name="mongo_src",
    host="atlascluster.ushabcd.mongodb.net",
    db_name="mongo",
    username="username",
    password="password",
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


def test_invalid_deltalake_cdc():
    with pytest.raises(ValueError) as e:

        @source(
            s3.bucket("data", prefix="user", format="delta"),
            every="1h",
            cdc="upsert",
            disorder="1d",
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

        view = InternalTestClient()
        view.add(UserInfoDataset)
        sync_request = view._get_sync_request_proto()
        assert len(sync_request.dataset_info) == 1
        dataset_request = sync_request.dataset_info[0]
        assert len(dataset_request.sources) == 3

    assert str(e.value) == "CDC must be set as native for delta format"


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
                at_timestamp(datetime.now(timezone.utc)),
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
def test_sink_on_source(client):
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @source(kafka.topic("test_topic"), disorder="14d", cdc="upsert")
        @sink(kafka.topic("test_topic_2"), cdc="debezium")
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
        == "Dataset `UserInfoDataset` error: Cannot define sinks on a source dataset"
    )


@mock
def test_invalid_sink(client):
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @source(kafka.topic("test_topic"), disorder="14d", cdc="append")
        @sink(kafka.topic("test_topic_2", format="csv"), cdc="debezium")
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
        == 'Sink only support "json" format for now, found debezium'
    )

    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @source(kafka.topic("test_topic"), disorder="14d", cdc="append")
        @sink(kafka.topic("test_topic_2", format="json"), cdc="native")
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

    assert str(e.value) == 'Sink only support "debezium" cdc, found native'

    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @source(kafka.topic("test_topic"), disorder="14d", cdc="append")
        @sink(
            kinesis.stream(
                "test_stream", format="json", init_position=datetime(2023, 1, 5)
            ),
            cdc="debezium",
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
        == "Sink only support Kafka Connector, found <class 'fennel.connectors.connectors.KinesisConnector'>"
    )


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
    service_account_key={},
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


def test_invalid_preproc_value():
    # Preproc value of "" cannot be set
    with pytest.raises(ValueError) as e:
        source(
            s3.bucket(
                bucket_name="all_ratings", prefix="prod/apac/", format="json"
            ),
            every="1h",
            disorder="14d",
            cdc="append",
            preproc={"C": ref("")},
        )
    assert (
        "Expected column name to be non empty inside preproc ref type"
        == str(e.value)
    )

    # Preproc value of "A[B[C]" cannot be set
    with pytest.raises(ValueError) as e:
        source(
            s3.bucket(
                bucket_name="all_ratings", prefix="prod/apac/", format="json"
            ),
            every="1h",
            disorder="14d",
            cdc="append",
            preproc={"C": ref("A[B[C]")},
        )
    assert (
        "Invalid preproc value of ref type, there is no closing ] for the corresponding opening ["
        == str(e.value)
    )

    # Preproc value of type A[B][C] cannot be set for data other than JSON format
    with pytest.raises(ValueError) as e:
        source(
            s3.bucket(
                bucket_name="all_ratings", prefix="prod/apac/", format="delta"
            ),
            every="1h",
            disorder="14d",
            cdc="native",
            preproc={"C": ref("A[B][C]"), "D": "A[B][C]"},
        )
    assert (
        "Preproc of type ref('A[B][C]') is applicable only for data in JSON format"
        == str(e.value)
    )

    # Preproc value of type A[B][C] cannot be set for data other than JSON format
    with pytest.raises(ValueError) as e:
        source(
            kafka.topic(topic="topic", format="Avro"),
            every="1h",
            disorder="14d",
            cdc="debezium",
            preproc={"C": ref("A[B][C]"), "D": "A[B][C]"},
        )
    assert (
        "Preproc of type ref('A[B][C]') is applicable only for data in JSON format"
        == str(e.value)
    )


def test_invalid_protobuf_args():
    with pytest.raises(ValueError) as e:
        Protobuf(
            registry="confluent",
            url="https://psrc-zj6ny.us-east-2.aws.confluent.cloud",
            username=None,
            password="password",
            token="token",
        )
    assert (
        "Token shouldn't be passed when using username/password based authentication"
        == str(e.value)
    )

    with pytest.raises(ValueError) as e:
        Protobuf(
            registry="confluent",
            url="https://psrc-zj6ny.us-east-2.aws.confluent.cloud",
            username="username",
            password=None,
            token="token",
        )
    assert (
        "Token shouldn't be passed when using username/password based authentication"
        == str(e.value)
    )

    with pytest.raises(ValueError) as e:
        Protobuf(
            registry="confluent",
            url="https://psrc-zj6ny.us-east-2.aws.confluent.cloud",
            username="username",
            password=None,
            token=None,
        )
    assert "Both username and password should be non-empty" == str(e.value)

    with pytest.raises(ValueError) as e:
        Protobuf(
            registry="confluent",
            url="https://psrc-zj6ny.us-east-2.aws.confluent.cloud",
            username=None,
            password="password",
            token=None,
        )
    assert "Both username and password should be non-empty" == str(e.value)

    with pytest.raises(ValueError) as e:
        Protobuf(
            registry="confluent",
            url="https://psrc-zj6ny.us-east-2.aws.confluent.cloud",
            username=None,
            password=None,
            token=None,
        )
    assert "Either username/password or token should be set" == str(e.value)
