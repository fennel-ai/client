from datetime import datetime

from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional

import fennel.gen.connector_pb2 as connector_proto
import fennel.gen.dataset_pb2 as ds_proto
from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.sources import (
    source,
    MySQL,
    S3,
    Snowflake,
    BigQuery,
    Kafka,
    Kinesis,
    InitPosition,
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


def test_simple_source():
    @source(
        mysql.table(
            "users",
            cursor="added_on",
        ),
        every="1h",
        lateness="20h",
    )
    @meta(owner="test@test.com")
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
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 1
    dataset_request = sync_request.datasets[0]
    d = {
        "name": "UserInfoDataset",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"int_type": {}},
                    }
                ],
            },
            "values": {
                "fields": [
                    {
                        "name": "name",
                        "dtype": {"string_type": {}},
                    },
                    {
                        "name": "gender",
                        "dtype": {"string_type": {}},
                    },
                    {
                        "name": "dob",
                        "dtype": {"string_type": {}},
                    },
                    {
                        "name": "age",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "account_creation_date",
                        "dtype": {"timestamp_type": {}},
                    },
                    {
                        "name": "country",
                        "dtype": {"optional_type": {"of": {"string_type": {}}}},
                    },
                ]
            },
            "timestamp": "timestamp",
        },
        "metadata": {
            "owner": "test@test.com",
        },
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "user_id": {},
            "name": {},
            "gender": {},
            "dob": {"description": "Users date of birth"},
            "age": {},
            "account_creation_date": {},
            "country": {},
            "timestamp": {},
        },
        "isSourceDataset": True,
    }
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    expected_dataset_request.pycode.Clear()
    dataset_request.pycode.Clear()
    assert dataset_request == expected_dataset_request, error_message(
        dataset_request, expected_dataset_request
    )

    assert len(sync_request.sources) == 1
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "mysql_table": {
                "db": {
                    "name": "mysql",
                    "mysql": {
                        "host": "localhost",
                        "database": "test",
                        "user": "root",
                        "password": "root",
                        "port": 3306,
                    },
                },
                "table_name": "users",
            },
        },
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "72000s",
        "cursor": "added_on",
        "timestamp_field": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    # External DBs
    assert len(sync_request.extdbs) == 1
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "mysql",
        "mysql": {
            "host": "localhost",
            "database": "test",
            "user": "root",
            "password": "root",
            "port": 3306,
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


s3 = S3(
    name="ratings_source",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
)

bigquery = BigQuery(
    name="bq_movie_tags",
    project_id="gold-cocoa-356105",
    dataset_id="movie_tags",
    credentials_json="""{
        "type": "service_account",
        "project_id": "fake-project-356105",
        "client_email": "randomstring@fake-project-356105.iam.gserviceaccount.com",
        "client_id": "103688493243243272951",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    }""",
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
    verify_cert=False,
)

s3_console = S3.get(
    name="s3_test",
)

kinesis = Kinesis(
    name="kinesis_src",
    role_arn="arn:aws:iam::123456789012:role/test-role",
)


def test_multiple_sources():
    @meta(owner="test@test.com")
    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
        ),
        every="1h",
        lateness="2d",
    )
    @dataset
    class UserInfoDatasetS3:
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
    view.add(UserInfoDatasetS3)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 1
    assert len(sync_request.extdbs) == 1

    # s3 source
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "s3Table": {
                "bucket": "all_ratings",
                "pathPrefix": "prod/apac/",
                "delimiter": ",",
                "format": "csv",
                "db": {
                    "name": "ratings_source",
                    "s3": {
                        "awsSecretAccessKey": "8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
                        "awsAccessKeyId": "ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
                    },
                },
            }
        },
        "dataset": "UserInfoDatasetS3",
        "every": "3600s",
        "lateness": "172800s",
        "timestamp_field": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "ratings_source",
        "s3": {
            "awsSecretAccessKey": "8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
            "awsAccessKeyId": "ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(snowflake.table("users_Sf", cursor="added_on"), every="1h")
    @dataset
    class UserInfoDatasetSnowFlake:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # snowflake source
    view = InternalTestClient()
    view.add(UserInfoDatasetSnowFlake)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "snowflakeTable": {
                "db": {
                    "snowflake": {
                        "account": "nhb38793.us-west-2.snowflakecomputing.com",
                        "user": "<username>",
                        "password": "<password>",
                        "schema": "PUBLIC",
                        "warehouse": "TEST",
                        "role": "ACCOUNTADMIN",
                        "database": "MOVIELENS",
                    },
                    "name": "snowflake_src",
                },
                "tableName": "users_Sf",
            }
        },
        "dataset": "UserInfoDatasetSnowFlake",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "snowflake_src",
        "snowflake": {
            "account": "nhb38793.us-west-2.snowflakecomputing.com",
            "user": "<username>",
            "password": "<password>",
            "schema": "PUBLIC",
            "warehouse": "TEST",
            "role": "ACCOUNTADMIN",
            "database": "MOVIELENS",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        bigquery.table("users_bq", cursor="added_on"), every="1h", lateness="2h"
    )
    @dataset
    class UserInfoDatasetBigQuery:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # bigquery source
    view = InternalTestClient()
    view.add(UserInfoDatasetBigQuery)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "bigqueryTable": {
                "db": {
                    "name": "bq_movie_tags",
                    "bigquery": {
                        "dataset": "movie_tags",
                        "credentialsJson": '{\n        "type": "service_account",\n        "project_id": "fake-project-356105",\n        "client_email": "randomstring@fake-project-356105.iam.gserviceaccount.com",\n        "client_id": "103688493243243272951",\n        "auth_uri": "https://accounts.google.com/o/oauth2/auth",\n        "token_uri": "https://oauth2.googleapis.com/token",\n        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",\n    }',
                        "projectId": "gold-cocoa-356105",
                    },
                },
                "tableName": "users_bq",
            }
        },
        "dataset": "UserInfoDatasetBigQuery",
        "every": "3600s",
        "lateness": "7200s",
        "cursor": "added_on",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "bq_movie_tags",
        "bigquery": {
            "dataset": "movie_tags",
            "credentialsJson": '{\n        "type": "service_account",\n        "project_id": "fake-project-356105",\n        "client_email": "randomstring@fake-project-356105.iam.gserviceaccount.com",\n        "client_id": "103688493243243272951",\n        "auth_uri": "https://accounts.google.com/o/oauth2/auth",\n        "token_uri": "https://oauth2.googleapis.com/token",\n        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",\n    }',
            "projectId": "gold-cocoa-356105",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(mysql.table("users_mysql", cursor="added_on"), every="1h")
    @dataset
    class UserInfoDatasetMySql:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # mysql source
    view = InternalTestClient()
    view.add(UserInfoDatasetMySql)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "mysql_table": {
                "db": {
                    "name": "mysql",
                    "mysql": {
                        "host": "localhost",
                        "database": "test",
                        "user": "root",
                        "password": "root",
                        "port": 3306,
                    },
                },
                "table_name": "users_mysql",
            },
        },
        "dataset": "UserInfoDatasetMySql",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "mysql",
        "mysql": {
            "host": "localhost",
            "database": "test",
            "user": "root",
            "password": "root",
            "port": 3306,
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(kafka.topic("test_topic"))
    @dataset
    class UserInfoDatasetKafka:
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
    view.add(UserInfoDatasetKafka)
    sync_request = view._get_sync_request_proto()
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "kafka_src",
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "test",
            "sasl_plain_password": "test",
            "enable_ssl_certificate_verification": False,
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(kinesis.stream("test_stream", InitPosition.LATEST))
    @dataset
    class UserInfoDatasetKinesis:
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
    view.add(UserInfoDatasetKinesis)
    sync_request = view._get_sync_request_proto()
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "kinesis_src",
        "kinesis": {"roleArn": "arn:aws:iam::123456789012:role/test-role"},
    }

    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        kinesis.stream(
            "test_stream",
            InitPosition.AT_TIMESTAMP,
            init_timestamp=datetime(2023, 5, 31, 15, 30),
            format="json",
        )
    )
    @dataset
    class UserInfoDatasetKinesis:
        user_id: int = field(key=True)
        name: str
        gender: str
        timestamp: datetime = field(timestamp=True)

    view = InternalTestClient()
    view.add(UserInfoDatasetKinesis)
    sync_request = view._get_sync_request_proto()

    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "kinesis_src",
        "kinesis": {"roleArn": "arn:aws:iam::123456789012:role/test-role"},
    }

    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    source_req = sync_request.sources[0]
    e = {
        "table": {
            "kinesisStream": {
                "streamArn": "test_stream",
                "initPosition": "AT_TIMESTAMP",
                "initTimestamp": "2023-05-31T15:30:00Z",
                "format": "json",
                "db": {
                    "name": "kinesis_src",
                    "kinesis": {
                        "roleArn": "arn:aws:iam::123456789012:role/test-role"
                    },
                },
            }
        },
        "dataset": "UserInfoDatasetKinesis",
        "lateness": "3600s",
    }
    expected_source = ParseDict(e, connector_proto.Source())
    assert source_req == expected_source, error_message(
        source_req, expected_source
    )


def test_console_source():
    @source(
        s3_console.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
        ),
        every="1h",
    )
    @meta(owner="test@test.com", tags=["test", "yolo"])
    @dataset
    class UserInfoDataset:
        user_id: int = field(key=True)
        timestamp: datetime = field(timestamp=True)

    view = InternalTestClient()
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 1
    assert len(sync_request.extdbs) == 1

    # last source
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "s3Table": {
                "bucket": "all_ratings",
                "pathPrefix": "prod/apac/",
                "delimiter": ",",
                "format": "csv",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "3600s",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {"s3": {}, "name": "s3_test"}
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )
