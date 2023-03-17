from datetime import datetime
from typing import Optional

from google.protobuf.json_format import ParseDict  # type: ignore

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
    Postgres,
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


def test_simple_source(grpc_stub):
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

    view = InternalTestClient(grpc_stub)
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
    }
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
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
)


def test_multiple_sources(grpc_stub):
    @meta(owner="test@test.com")
    @source(kafka.topic("test_topic"))
    @source(mysql.table("users_mysql", cursor="added_on"), every="1h")
    @source(
        bigquery.table("users_bq", cursor="added_on"), every="1h", lateness="2h"
    )
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

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 5
    assert len(sync_request.extdbs) == 5

    # last source
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
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "172800s",
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

    # third source
    source_request = sync_request.sources[1]
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
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[1]
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

    # second source
    source_request = sync_request.sources[2]
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
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "7200s",
        "cursor": "added_on",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[2]
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

    # first source
    source_request = sync_request.sources[3]
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
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[3]
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
    extdb_request = sync_request.extdbs[4]
    e = {
        "name": "kafka_src",
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "security_protocol": "PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "test",
            "sasl_plain_password": "test",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


posgres_console = Postgres.get(
    name="posgres_test",
)
mysql_console = MySQL.get(
    name="mysql_test",
)
snowflake_console = Snowflake.get(
    name="snowflake_test",
)
bigquery_console = BigQuery.get(
    name="bigquery_test",
)
s3_console = S3.get(
    name="s3_test",
)


def test_console_source(grpc_stub):
    @source(posgres_console.table("users", cursor="added_on"), every="1h")
    @source(mysql_console.table("users", cursor="added_on"), every="1h")
    @source(snowflake_console.table("users", cursor="added_on"), every="1h")
    @source(bigquery_console.table("users", cursor="added_on"), every="1h")
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

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 5
    assert len(sync_request.extdbs) == 5

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

    # 4th source
    source_request = sync_request.sources[1]
    s = {
        "table": {
            "bigqueryTable": {
                "db": {"bigquery": {}, "name": "bigquery_test"},
                "tableName": "users",
            }
        },
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "cursor": "added_on",
        "lateness": "3600s",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[1]
    e = {"bigquery": {}, "name": "bigquery_test"}
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    # 3rd source
    source_request = sync_request.sources[2]
    s = {
        "table": {
            "snowflakeTable": {
                "db": {"snowflake": {}, "name": "snowflake_test"},
                "tableName": "users",
            }
        },
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[2]
    e = {"snowflake": {}, "name": "snowflake_test"}
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    source_request = sync_request.sources[3]
    s = {
        "table": {
            "mysqlTable": {
                "db": {"mysql": {"port": 3306}, "name": "mysql_test"},
                "tableName": "users",
            }
        },
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[3]
    e = {"mysql": {"port": 3306}, "name": "mysql_test"}
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    source_request = sync_request.sources[4]
    s = {
        "table": {
            "pgTable": {
                "db": {"postgres": {"port": 5432}, "name": "posgres_test"},
                "tableName": "users",
            }
        },
        "dataset": "UserInfoDataset",
        "every": "3600s",
        "lateness": "3600s",
        "cursor": "added_on",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[4]
    e = {"postgres": {"port": 5432}, "name": "posgres_test"}
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )
