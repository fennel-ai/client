from datetime import datetime
from typing import Optional

import pytest
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.connector_pb2 as connector_proto
import fennel.gen.dataset_pb2 as ds_proto
from fennel.datasets import dataset, field
from fennel.lib import meta
from fennel.sources import (
    source,
    MySQL,
    S3,
    Snowflake,
    BigQuery,
    Kafka,
    Kinesis,
    Avro,
    ref,
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


def test_simple_source():
    @source(
        mysql.table(
            "users",
            cursor="added_on",
        ),
        every="1h",
        cdc="append",
        disorder="20h",
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
        "version": 1,
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
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "72000s",
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


def test_simple_source_with_pre_proc():
    @source(
        mysql.table(
            "users",
            cursor="added_on",
        ),
        every="1h",
        disorder="20h",
        cdc="append",
        preproc={
            "age": 10,
            "gender": "male",
            "timestamp": datetime(1970, 1, 1, 0, 0, 0),
            "country": ref("upstream.country"),
        },
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
        "version": 1,
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
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "72000s",
        "cursor": "added_on",
        "timestamp_field": "timestamp",
        "pre_proc": {
            "age": {
                "value": {
                    "int": 10,
                },
            },
            "country": {
                "ref": "upstream.country",
            },
            "gender": {
                "value": {
                    "string": "male",
                }
            },
            "timestamp": {
                "value": {"timestamp": "1970-01-01T00:00:00Z"},
            },
        },
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
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
    }""",
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
    security_protocol="PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="test",
    sasl_plain_password="test",
)

s3_console = S3.get(
    name="s3_test",
)

kinesis = Kinesis(
    name="kinesis_src",
    role_arn="arn:aws:iam::123456789012:role/test-role",
)


def test_tier_selector_on_source():
    @meta(owner="test@test.com")
    @source(
        kafka.topic("test_topic"), disorder="14d", cdc="append", tier=["dev-2"]
    )
    @source(
        mysql.table("users_mysql", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="append",
        tier=["prod"],
    )
    @source(
        snowflake.table("users_Sf", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="append",
        tier=["staging"],
    )
    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
        ),
        every="1h",
        disorder="2d",
        cdc="append",
        tier=["dev"],
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
    with pytest.raises(ValueError) as e:
        view._get_sync_request_proto()
    assert (
        str(e.value)
        == "Dataset UserInfoDataset has multiple sources (4) defined. Please define only one source per dataset, or check your tier selection."
    )
    sync_request = view._get_sync_request_proto(tier="prod")
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 1
    assert len(sync_request.extdbs) == 1
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "mysqlTable": {
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
                "tableName": "users_mysql",
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "cursor": "added_on",
        "disorder": "1209600s",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    sync_request = view._get_sync_request_proto(tier="staging")
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 1
    assert len(sync_request.extdbs) == 1
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "snowflakeTable": {
                "db": {
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
                },
                "tableName": "users_Sf",
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "cursor": "added_on",
        "disorder": "1209600s",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )


def test_multiple_sources():
    @meta(owner="test@test.com")
    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
            presorted=True,
            format="delta",
        ),
        every="1h",
        disorder="2d",
        cdc="append",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        until=datetime.strptime("2022-02-28T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
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
                "pathSuffix": "",
                "delimiter": ",",
                "format": "delta",
                "preSorted": True,
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
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "172800s",
        "startingFrom": "2021-08-10T00:00:00Z",
        "until": "2022-02-28T00:00:00Z",
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
    @source(
        snowflake.table("users_Sf", cursor="added_on"),
        disorder="14d",
        cdc="append",
        every="1h",
    )
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
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "1209600s",
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
        snowflake.table("users_Sf", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="append",
        until=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetSnowFlakeStartingFrom:
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
    view.add(UserInfoDatasetSnowFlakeStartingFrom)
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
        "dataset": "UserInfoDatasetSnowFlakeStartingFrom",
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "1209600s",
        "cursor": "added_on",
        "timestampField": "timestamp",
        "until": "2021-08-10T00:00:00Z",
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
        bigquery.table("users_bq", cursor="added_on"),
        every="1h",
        disorder="2h",
        cdc="append",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
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
                        "credentialsJson": '{\n        "type": "service_account",\n        "project_id": "fake-project-356105",\n        "client_email": "randomstring@fake-project-356105.iam.gserviceaccount.com",\n        "client_id": "103688493243243272951",\n        "auth_uri": "https://accounts.google.com/o/oauth2/auth",\n        "token_uri": "https://oauth2.googleapis.com/token",\n        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"\n    }',
                        "projectId": "gold-cocoa-356105",
                    },
                },
                "tableName": "users_bq",
            }
        },
        "dataset": "UserInfoDatasetBigQuery",
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "7200s",
        "cursor": "added_on",
        "timestampField": "timestamp",
        "startingFrom": "2021-08-10T00:00:00Z",
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
            "credentialsJson": '{\n        "type": "service_account",\n        "project_id": "fake-project-356105",\n        "client_email": "randomstring@fake-project-356105.iam.gserviceaccount.com",\n        "client_id": "103688493243243272951",\n        "auth_uri": "https://accounts.google.com/o/oauth2/auth",\n        "token_uri": "https://oauth2.googleapis.com/token",\n        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"\n    }',
            "projectId": "gold-cocoa-356105",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        mysql.table("users_mysql", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="append",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
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
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "1209600s",
        "cursor": "added_on",
        "timestampField": "timestamp",
        "startingFrom": "2021-08-10T00:00:00Z",
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
    @source(
        kafka.topic("test_topic"),
        disorder="14d",
        cdc="append",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
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
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "kafkaTopic": {
                "db": {
                    "name": "kafka_src",
                    "kafka": {
                        "bootstrapServers": "localhost:9092",
                        "securityProtocol": "PLAINTEXT",
                        "saslMechanism": "PLAIN",
                        "saslPlainUsername": "test",
                        "saslPlainPassword": "test",
                    },
                },
                "topic": "test_topic",
                "format": {
                    "json": {},
                },
            }
        },
        "dataset": "UserInfoDatasetKafka",
        "dsVersion": 1,
        "disorder": "1209600s",
        "startingFrom": "2021-08-10T00:00:00Z",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    avro = Avro(
        registry="confluent",
        url="http://localhost:8000",
        username="user",
        password="pwd",
    )

    @meta(owner="test@test.com")
    @source(
        kafka.topic(
            "test_topic",
            format=avro,
        ),
        cdc="debezium",
        disorder="14d",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
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
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "kafkaTopic": {
                "db": {
                    "name": "kafka_src",
                    "kafka": {
                        "bootstrapServers": "localhost:9092",
                        "securityProtocol": "PLAINTEXT",
                        "saslMechanism": "PLAIN",
                        "saslPlainUsername": "test",
                        "saslPlainPassword": "test",
                    },
                },
                "topic": "test_topic",
                "format": {
                    "avro": {
                        "schemaRegistry": {
                            "url": "http://localhost:8000",
                            "auth": {
                                "basic": {"username": "user", "password": "pwd"}
                            },
                        }
                    }
                },
            }
        },
        "dataset": "UserInfoDatasetKafka",
        "dsVersion": 1,
        "disorder": "1209600s",
        "cdc": "Debezium",
        "startingFrom": "2021-08-10T00:00:00Z",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    @meta(owner="test@test.com")
    @source(
        kinesis.stream("test_stream", init_position="latest", format="json"),
        disorder="14d",
        cdc="append",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
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
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "kinesisStream": {
                "streamArn": "test_stream",
                "initPosition": "LATEST",
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
        "dsVersion": 1,
        "disorder": "1209600s",
        "startingFrom": "2021-08-10T00:00:00Z",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    @meta(owner="test@test.com")
    @source(
        kinesis.stream(
            "test_stream",
            init_position="2023-05-31 15:30:00",
            format="json",
        ),
        disorder="14d",
        cdc="append",
        since=datetime(2023, 5, 31),
    )
    @dataset
    class UserInfoDatasetKinesis:
        user_id: int = field(key=True)
        name: str
        gender: str
        timestamp: datetime = field(timestamp=True)

    @meta(owner="test@test.com")
    @source(
        kinesis.stream(
            "test_stream2",
            init_position=datetime(2023, 5, 31),
            format="json",
        ),
        disorder="14d",
        cdc="append",
    )
    @dataset
    class UserInfoDatasetKinesis2:
        user_id: int = field(key=True)
        name: str
        gender: str
        timestamp: datetime = field(timestamp=True)

    view = InternalTestClient()
    view.add(UserInfoDatasetKinesis)
    view.add(UserInfoDatasetKinesis2)
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
        "dsVersion": 1,
        "disorder": "1209600s",
        "startingFrom": "2023-05-31T00:00:00Z",
    }
    expected_source = ParseDict(e, connector_proto.Source())
    assert source_req == expected_source, error_message(
        source_req, expected_source
    )

    source_req = sync_request.sources[1]
    e = {
        "table": {
            "kinesisStream": {
                "streamArn": "test_stream2",
                "initPosition": "AT_TIMESTAMP",
                "initTimestamp": "2023-05-31T00:00:00Z",
                "format": "json",
                "db": {
                    "name": "kinesis_src",
                    "kinesis": {
                        "roleArn": "arn:aws:iam::123456789012:role/test-role"
                    },
                },
            }
        },
        "dataset": "UserInfoDatasetKinesis2",
        "dsVersion": 1,
        "disorder": "1209600s",
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
        disorder="14d",
        cdc="append",
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
                "pathSuffix": "",
                "delimiter": ",",
                "format": "csv",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "1209600s",
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


def test_s3_source_with_path():
    @source(
        s3_console.bucket(
            bucket_name="all_ratings",
            path="prod/data_type=events/*/date=%Y%m%d/hour=%H/*/*.csv",
        ),
        disorder="14d",
        cdc="append",
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
                "pathPrefix": "prod/data_type=events/",
                "pathSuffix": "*/date=%Y%m%d/hour=%H/*/*.csv",
                "delimiter": ",",
                "format": "csv",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "1209600s",
        "timestampField": "timestamp",
        "bounded": False,
        "idleness": None,
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

    # Test with spread
    @source(
        s3_console.bucket(
            bucket_name="all_ratings",
            path="prod/data_type=events/*/date=%Y%m%d/hour=%H/*/*.csv",
            spread="6h",
        ),
        disorder="14d",
        cdc="append",
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
                "pathPrefix": "prod/data_type=events/",
                "pathSuffix": "*/date=%Y%m%d/hour=%H/*/*.csv",
                "delimiter": ",",
                "format": "csv",
                "spread": "21600s",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "disorder": "1209600s",
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

    # see test_invalid_sources for invalid examples
    valid_path_cases = [
        ("fixed/foo/bar/", "fixed/foo/bar/", ""),
        ("foo/", "foo/", ""),
        ("foo/*/*/*", "foo/", "*/*/*"),
        ("foo/*/*.json", "foo/", "*/*.json"),
        ("*/*.json", "", "*/*.json"),
        ("foo/%Y/%m/%d/*.json", "foo/", "%Y/%m/%d/*.json"),
        ("foo/name=bar/%Y/%m/%d/*.json", "foo/name=bar/", "%Y/%m/%d/*.json"),
        (
            "foo/bar/baz/*/%Y-%m-%d/%H/*/*.csv",
            "foo/bar/baz/",
            "*/%Y-%m-%d/%H/*/*.csv",
        ),
        (
            "*/year=%Y/month=%m/day=%d/hour=%H/*/*",
            "",
            "*/year=%Y/month=%m/day=%d/hour=%H/*/*",
        ),
        ("foo/**", "foo/", "**"),
        ("**/*", "", "**/*"),
        (
            "*/year-%Y/weel-%W/%d/hour-%H/*/*",
            "",
            "*/year-%Y/weel-%W/%d/hour-%H/*/*",
        ),
        (
            "foo/bar/year=%Y/*/*/day=%d/*/*.csv.bz2",
            "foo/bar/",
            "year=%Y/*/*/day=%d/*/*.csv.bz2",
        ),
        ("foo/bar/baz.json", "foo/bar/baz.json", ""),
        ("foo-bar/baz-fun.json", "foo-bar/baz-fun.json", ""),
        ("*/year=%Y", "", "*/year=%Y"),
        ("%Y%m%d/*", "", "%Y%m%d/*"),
        ("foo/**/hh=%H/*", "foo/", "**/hh=%H/*"),
    ]

    for path, expected_prefix, expected_suffix in valid_path_cases:
        prefix, suffix = S3Connector.parse_path(path)
        assert (
            prefix == expected_prefix
        ), f"Expected prefix: {expected_prefix}, got: {prefix}"
        assert (
            suffix == expected_suffix
        ), f"Expected suffix: {expected_suffix}, got: {suffix}"


def test_bounded_source_with_idleness():
    @source(
        mysql.table(
            "users",
            cursor="added_on",
        ),
        every="1h",
        disorder="20h",
        bounded=True,
        idleness="1h",
        cdc="append",
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
        "version": 1,
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
        "disorder": "72000s",
        "bounded": True,
        "idleness": "3600s",
        "cursor": "added_on",
        "timestamp_field": "timestamp",
        "dsVersion": 1,
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
