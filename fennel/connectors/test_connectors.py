import sys
from datetime import datetime
from typing import Optional

import pandas as pd
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.connector_pb2 as connector_proto
import fennel.gen.dataset_pb2 as ds_proto
from fennel.connectors import (
    source,
    Mongo,
    sink,
    MySQL,
    S3,
    Redshift,
    Snowflake,
    BigQuery,
    Kafka,
    Kinesis,
    Avro,
    Protobuf,
    ref,
    S3Connector,
    PubSub,
    eval,
)
from fennel.connectors.connectors import CSV, Postgres
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.expr import col, lit
from fennel.integrations.aws import Secret
from fennel.lib import meta
from fennel.lib.params import inputs

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
        cdc="upsert",
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
        "cdc": "Upsert",
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
        cdc="upsert",
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
        "cdc": "Upsert",
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


aws_secret = Secret(
    arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
    role_arn="arn:aws:iam::123456789012:role/fennel-test-role",
)

s3 = S3(
    name="ratings_source",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
)

s3_with_secret = S3(
    name="ratings_source",
    aws_access_key_id=aws_secret["s3_aws_access_key_id"],
    aws_secret_access_key=aws_secret["s3_aws_secret_access_key"],
)

simple_s3 = S3(name="my_s3_src")

mysql_with_secret = MySQL(
    name="mysql",
    host="localhost",
    db_name="test",
    username=aws_secret["mysql_username"],
    password=aws_secret["mysql_password"],
)

postgres = Postgres(
    name="postgres",
    host="localhost",
    db_name="test",
    username="root",
    password="root",
)

postgres_with_secret = Postgres(
    name="postgres",
    host="localhost",
    db_name="test",
    username=aws_secret["postgres_username"],
    password=aws_secret["postgres_password"],
)

bigquery = BigQuery(
    name="bq_movie_tags",
    project_id="gold-cocoa-356105",
    dataset_id="movie_tags",
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

bigquery_with_secret = BigQuery(
    name="bq_movie_tags",
    project_id="gold-cocoa-356105",
    dataset_id="movie_tags",
    service_account_key=aws_secret["bigquery_service_account_key"],
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

snowflake_with_secret = Snowflake(
    name="snowflake_src",
    account="nhb38793.us-west-2.snowflakecomputing.com",
    warehouse="TEST",
    db_name="MOVIELENS",
    schema="PUBLIC",
    role="ACCOUNTADMIN",
    username=aws_secret["us-west-2"]["username"],
    password=aws_secret["us-west-2"]["password"],
)

kafka = Kafka(
    name="kafka_src",
    bootstrap_servers="localhost:9092",
    security_protocol="PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="test",
    sasl_plain_password="test",
)

kafka_with_secret = Kafka(
    name="kafka_src",
    bootstrap_servers="localhost:9092",
    security_protocol="PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=aws_secret["sasl_plain_username"],
    sasl_plain_password=aws_secret["sasl_plain_password"],
)

s3_console = S3.get(
    name="s3_test",
)

kinesis = Kinesis(
    name="kinesis_src",
    role_arn="arn:aws:iam::123456789012:role/test-role",
)

redshift = Redshift(
    name="redshift_src",
    s3_access_role_arn="arn:aws:iam::123:role/Redshift",
    db_name="test",
    host="test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
    schema="public",
)

redshift2 = Redshift(
    name="redshift_src_2",
    db_name="test",
    host="test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
    schema="public",
    username="username",
    password="password",
)

redshift_with_secret = Redshift(
    name="redshift_src_2",
    db_name="test",
    host="test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
    schema="public",
    username=aws_secret["redshift_username"],
    password=aws_secret["redshift_password"],
)

mongo = Mongo(
    name="mongo_src",
    host="atlascluster.ushabcd.mongodb.net",
    db_name="mongo",
    username="username",
    password="password",
)

mongo_with_secret = Mongo(
    name="mongo_src",
    host="atlascluster.ushabcd.mongodb.net",
    db_name="mongo",
    username=aws_secret["mongo_username"],
    password=aws_secret["mongo_password"],
)

mongo_with_only_password_secret = Mongo(
    name="mongo_src",
    host="atlascluster.ushabcd.mongodb.net",
    db_name="mongo",
    username="username",
    password=aws_secret["mongo_password"],
)

pubsub = PubSub(
    name="pubsub_src",
    project_id="test_project",
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

pubsub_with_secret = PubSub(
    name="pubsub_src",
    project_id="test_project",
    service_account_key=aws_secret["pubsub_service_account_key"],
)


def test_env_selector_on_connector():
    @meta(owner="test@test.com")
    @source(
        pubsub.topic("test_topic", format="json"),
        disorder="2d",
        cdc="append",
        env=["dev-3"],
    )
    @source(
        mongo.collection("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
        env=["dev-3"],
    )
    @source(
        redshift.table("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
        env=["dev-3"],
    )
    @source(
        kafka.topic("test_topic"), disorder="14d", cdc="upsert", env=["dev-2"]
    )
    @source(
        mysql.table("users_mysql", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
        env=["prod"],
    )
    @source(
        snowflake.table("users_Sf", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
        env=["staging"],
    )
    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
        ),
        every="1h",
        disorder="2d",
        cdc="upsert",
        env=["dev"],
    )
    @source(
        kafka.topic("test_topic"),
        disorder="14d",
        cdc="upsert",
        env=["prod_new"],
    )
    @source(
        kafka.topic("test_topic"),
        disorder="14d",
        cdc="upsert",
        env=["prod_new2"],
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

    @meta(owner="test@test.com")
    @sink(
        kafka.topic("test_topic1"),
        cdc="debezium",
        env=["prod"],
    )
    @sink(
        kafka.topic("test_topic2"),
        cdc="debezium",
        env=["staging"],
    )
    @sink(
        simple_s3.bucket("random_bucket", prefix="prod/apac/", format="delta"),
        every="1d",
        how="incremental",
        renames={"uid": "new_uid"},
        env=["prod_new"],
    )
    @sink(
        snowflake.table("random_table"),
        every="1d",
        how="incremental",
        renames={"uid": "new_uid"},
        env=["prod_new2"],
    )
    @dataset
    class UserInfoDatasetDerived:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

        @pipeline
        @inputs(UserInfoDataset)
        def create_user_transactions(cls, dataset: Dataset):
            return dataset

    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfoDatasetDerived)
    sync_request = view._get_sync_request_proto(env="prod")
    assert len(sync_request.datasets) == 2
    assert len(sync_request.sources) == 1
    assert len(sync_request.sinks) == 1
    assert len(sync_request.extdbs) == 2
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
        "cdc": "Upsert",
        "disorder": "1209600s",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    sink_request = sync_request.sinks[0]
    s = {
        "table": {
            "kafka_topic": {
                "db": {
                    "name": "kafka_src",
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "security_protocol": "PLAINTEXT",
                        "sasl_mechanism": "PLAIN",
                        "sasl_plain_username": "test",
                        "sasl_plain_password": "test",
                    },
                },
                "topic": "test_topic1",
                "format": {"json": {}},
            }
        },
        "dataset": "UserInfoDatasetDerived",
        "dsVersion": 1,
        "cdc": "Debezium",
    }
    expected_sink_request = ParseDict(s, connector_proto.Sink())
    assert sink_request == expected_sink_request, error_message(
        sink_request, expected_sink_request
    )
    sync_request = view._get_sync_request_proto(env="staging")
    assert len(sync_request.datasets) == 2
    assert len(sync_request.sources) == 1
    assert len(sync_request.sinks) == 1
    assert len(sync_request.extdbs) == 2
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
        "cdc": "Upsert",
        "disorder": "1209600s",
        "timestampField": "timestamp",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    sink_request = sync_request.sinks[0]
    s = {
        "table": {
            "kafka_topic": {
                "db": {
                    "name": "kafka_src",
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "security_protocol": "PLAINTEXT",
                        "sasl_mechanism": "PLAIN",
                        "sasl_plain_username": "test",
                        "sasl_plain_password": "test",
                    },
                },
                "topic": "test_topic2",
                "format": {"json": {}},
            }
        },
        "dataset": "UserInfoDatasetDerived",
        "dsVersion": 1,
        "cdc": "Debezium",
    }
    expected_sink_request = ParseDict(s, connector_proto.Sink())
    assert sink_request == expected_sink_request, error_message(
        sink_request, expected_sink_request
    )

    sync_request = view._get_sync_request_proto(env="prod_new")
    assert len(sync_request.datasets) == 2
    assert len(sync_request.sources) == 1
    assert len(sync_request.sinks) == 1
    assert len(sync_request.extdbs) == 2

    sink_request = sync_request.sinks[0]
    s = {
        "table": {
            "s3Table": {
                "bucket": "random_bucket",
                "pathPrefix": "prod/apac/",
                "format": "delta",
                "db": {
                    "name": "my_s3_src",
                    "s3": {},
                },
            }
        },
        "dataset": "UserInfoDatasetDerived",
        "dsVersion": 1,
        "every": "86400s",
        "how": {"incremental": {}},
        "create": True,
    }
    expected_sink_request = ParseDict(s, connector_proto.Sink())
    assert sink_request == expected_sink_request, error_message(
        sink_request, expected_sink_request
    )

    sync_request = view._get_sync_request_proto(env="prod_new2")
    assert len(sync_request.datasets) == 2
    assert len(sync_request.sources) == 1
    assert len(sync_request.sinks) == 1
    assert len(sync_request.extdbs) == 2

    sink_request = sync_request.sinks[0]
    s = {
        "table": {
            "snowflake_table": {
                "db": {
                    "name": "snowflake_src",
                    "snowflake": {
                        "user": "<username>",
                        "password": "<password>",
                        "account": "nhb38793.us-west-2.snowflakecomputing.com",
                        "schema": "PUBLIC",
                        "warehouse": "TEST",
                        "role": "ACCOUNTADMIN",
                        "database": "MOVIELENS",
                    },
                },
                "table_name": "random_table",
            },
        },
        "dataset": "UserInfoDatasetDerived",
        "dsVersion": 1,
        "every": "86400s",
        "how": {"incremental": {}},
        "create": True,
    }
    expected_sink_request = ParseDict(s, connector_proto.Sink())
    assert sink_request == expected_sink_request, error_message(
        sink_request, expected_sink_request
    )


def test_kafka_sink_and_source_doesnt_create_extra_extdbs():
    @meta(owner="test@test.com")
    @source(
        kafka.topic("test_topic"), disorder="14d", cdc="upsert", env=["prod"]
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

    @meta(owner="test@test.com")
    @sink(
        kafka.topic("test_topic1"),
        cdc="debezium",
        env=["prod"],
    )
    @dataset
    class UserInfoDatasetDerived:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

        @pipeline
        @inputs(UserInfoDataset)
        def create_user_transactions(cls, dataset: Dataset):
            return dataset

    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfoDatasetDerived)
    sync_request = view._get_sync_request_proto(env="prod")
    assert len(sync_request.datasets) == 2
    assert len(sync_request.sources) == 1
    assert len(sync_request.sinks) == 1
    assert len(sync_request.extdbs) == 1


def test_multiple_sinks():
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
        cdc="native",
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

    @meta(owner="test@test.com")
    @sink(
        kafka.topic("test_topic2"),
        cdc="debezium",
    )
    @dataset
    class UserInfoDatasetDerived:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

        @pipeline
        @inputs(UserInfoDatasetS3)
        def create_user_transactions(cls, dataset: Dataset):
            return dataset

    view = InternalTestClient()
    view.add(UserInfoDatasetS3)
    view.add(UserInfoDatasetDerived)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 2
    assert len(sync_request.sinks) == 1
    assert len(sync_request.extdbs) == 2

    # kafka sink
    sink_request = sync_request.sinks[0]
    s = {
        "table": {
            "kafka_topic": {
                "db": {
                    "name": "kafka_src",
                    "kafka": {
                        "bootstrap_servers": "localhost:9092",
                        "security_protocol": "PLAINTEXT",
                        "sasl_mechanism": "PLAIN",
                        "sasl_plain_username": "test",
                        "sasl_plain_password": "test",
                    },
                },
                "topic": "test_topic2",
                "format": {"json": {}},
            }
        },
        "dataset": "UserInfoDatasetDerived",
        "dsVersion": 1,
        "cdc": "Debezium",
    }
    expected_sink_request = ParseDict(s, connector_proto.Sink())
    assert sink_request == expected_sink_request, error_message(
        sink_request, expected_sink_request
    )
    extdb_request = sync_request.extdbs[1]
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


def test_multiple_sources_mysql():
    @meta(owner="test@test.com")
    @source(
        mysql.table("users_mysql", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
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
        "cdc": "Upsert",
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
        mysql_with_secret.table("users_mysql", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetMySqlWithSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # mysql source with secret
    view = InternalTestClient()
    view.add(UserInfoDatasetMySqlWithSecret)
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
                        "usernameSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["mysql_username"],
                        },
                        "passwordSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["mysql_password"],
                        },
                        "port": 3306,
                    },
                },
                "table_name": "users_mysql",
            },
        },
        "dataset": "UserInfoDatasetMySqlWithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
            "usernameSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["mysql_username"],
            },
            "passwordSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["mysql_password"],
            },
            "port": 3306,
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_postgres():
    @meta(owner="test@test.com")
    @source(
        postgres.table("users_postgres", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetPostgres:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # postgres source
    view = InternalTestClient()
    view.add(UserInfoDatasetPostgres)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "pg_table": {
                "db": {
                    "name": "postgres",
                    "postgres": {
                        "host": "localhost",
                        "database": "test",
                        "user": "root",
                        "password": "root",
                        "port": 5432,
                    },
                },
                "table_name": "users_postgres",
            },
        },
        "dataset": "UserInfoDatasetPostgres",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "postgres",
        "postgres": {
            "host": "localhost",
            "database": "test",
            "user": "root",
            "password": "root",
            "port": 5432,
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        postgres_with_secret.table("users_postgres", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetPostgresWithSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # postgres source with secret
    view = InternalTestClient()
    view.add(UserInfoDatasetPostgresWithSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "pg_table": {
                "db": {
                    "name": "postgres",
                    "postgres": {
                        "host": "localhost",
                        "database": "test",
                        "usernameSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["postgres_username"],
                        },
                        "passwordSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["postgres_password"],
                        },
                        "port": 5432,
                    },
                },
                "table_name": "users_postgres",
            },
        },
        "dataset": "UserInfoDatasetPostgresWithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "postgres",
        "postgres": {
            "host": "localhost",
            "database": "test",
            "usernameSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["postgres_username"],
            },
            "passwordSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["postgres_password"],
            },
            "port": 5432,
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_mongo():
    @meta(owner="test@test.com")
    @source(
        mongo.collection("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetMongo:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # mongo source
    view = InternalTestClient()
    view.add(UserInfoDatasetMongo)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]

    s = {
        "table": {
            "mongoCollection": {
                "db": {
                    "mongo": {
                        "host": "atlascluster.ushabcd.mongodb.net",
                        "database": "mongo",
                        "user": "username",
                        "password": "password",
                    },
                    "name": "mongo_src",
                },
                "collectionName": "test_table",
            }
        },
        "dataset": "UserInfoDatasetMongo",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "mongo_src",
        "mongo": {
            "host": "atlascluster.ushabcd.mongodb.net",
            "database": "mongo",
            "user": "username",
            "password": "password",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        mongo_with_secret.collection("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetMongoWithSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # mongo source with secret
    view = InternalTestClient()
    view.add(UserInfoDatasetMongoWithSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]

    s = {
        "table": {
            "mongoCollection": {
                "db": {
                    "mongo": {
                        "host": "atlascluster.ushabcd.mongodb.net",
                        "database": "mongo",
                        "usernameSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["mongo_username"],
                        },
                        "passwordSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["mongo_password"],
                        },
                    },
                    "name": "mongo_src",
                },
                "collectionName": "test_table",
            }
        },
        "dataset": "UserInfoDatasetMongoWithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "mongo_src",
        "mongo": {
            "host": "atlascluster.ushabcd.mongodb.net",
            "database": "mongo",
            "usernameSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["mongo_username"],
            },
            "passwordSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["mongo_password"],
            },
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        mongo_with_only_password_secret.collection(
            "test_table", cursor="added_on"
        ),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetMongoWithPasswordSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # mongo source with only password secret
    view = InternalTestClient()
    view.add(UserInfoDatasetMongoWithPasswordSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]

    s = {
        "table": {
            "mongoCollection": {
                "db": {
                    "mongo": {
                        "host": "atlascluster.ushabcd.mongodb.net",
                        "database": "mongo",
                        "user": "username",
                        "passwordSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["mongo_password"],
                        },
                    },
                    "name": "mongo_src",
                },
                "collectionName": "test_table",
            }
        },
        "dataset": "UserInfoDatasetMongoWithPasswordSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "mongo_src",
        "mongo": {
            "host": "atlascluster.ushabcd.mongodb.net",
            "database": "mongo",
            "user": "username",
            "passwordSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["mongo_password"],
            },
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_redshift():
    @meta(owner="test@test.com")
    @source(
        redshift.table("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetRedshift:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # redshift source
    view = InternalTestClient()
    view.add(UserInfoDatasetRedshift)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "redshiftTable": {
                "db": {
                    "redshift": {
                        "redshiftAuthentication": {
                            "s3_access_role_arn": "arn:aws:iam::123:role/Redshift"
                        },
                        "database": "test",
                        "host": "test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
                        "port": 5439,
                        "schema": "public",
                    },
                    "name": "redshift_src",
                },
                "tableName": "test_table",
            }
        },
        "dataset": "UserInfoDatasetRedshift",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "redshift_src",
        "redshift": {
            "redshiftAuthentication": {
                "s3_access_role_arn": "arn:aws:iam::123:role/Redshift"
            },
            "database": "test",
            "host": "test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
            "port": 5439,
            "schema": "public",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        redshift2.table("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetRedshiftUsingCreds:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # redshift source
    view = InternalTestClient()
    view.add(UserInfoDatasetRedshiftUsingCreds)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "redshiftTable": {
                "db": {
                    "redshift": {
                        "database": "test",
                        "host": "test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
                        "port": 5439,
                        "schema": "public",
                        "redshiftAuthentication": {
                            "credentials": {
                                "username": "username",
                                "password": "password",
                            }
                        },
                    },
                    "name": "redshift_src_2",
                },
                "tableName": "test_table",
            }
        },
        "dataset": "UserInfoDatasetRedshiftUsingCreds",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "redshift_src_2",
        "redshift": {
            "database": "test",
            "host": "test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
            "port": 5439,
            "schema": "public",
            "redshiftAuthentication": {
                "credentials": {"username": "username", "password": "password"}
            },
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        redshift_with_secret.table("test_table", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetRedshiftUsingCredsWithSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # redshift source
    view = InternalTestClient()
    view.add(UserInfoDatasetRedshiftUsingCredsWithSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "redshiftTable": {
                "db": {
                    "redshift": {
                        "database": "test",
                        "host": "test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
                        "port": 5439,
                        "schema": "public",
                        "redshiftAuthentication": {
                            "credentials": {
                                "usernameSecret": {
                                    "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                    "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                    "path": ["redshift_username"],
                                },
                                "passwordSecret": {
                                    "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                    "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                    "path": ["redshift_password"],
                                },
                            }
                        },
                    },
                    "name": "redshift_src_2",
                },
                "tableName": "test_table",
            }
        },
        "dataset": "UserInfoDatasetRedshiftUsingCredsWithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        "name": "redshift_src_2",
        "redshift": {
            "database": "test",
            "host": "test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
            "port": 5439,
            "schema": "public",
            "redshiftAuthentication": {
                "credentials": {
                    "usernameSecret": {
                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                        "path": ["redshift_username"],
                    },
                    "passwordSecret": {
                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                        "path": ["redshift_password"],
                    },
                }
            },
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_snowflake():

    @meta(owner="test@test.com")
    @source(
        snowflake.table("users_Sf", cursor="added_on"),
        every="1h",
        disorder="14d",
        cdc="upsert",
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
        "cdc": "Upsert",
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
        snowflake.table("users_Sf", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
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
        "cdc": "Upsert",
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
        snowflake_with_secret.table("users_Sf", cursor="added_on"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetSnowFlakeWithSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # snowflake source with secret
    view = InternalTestClient()
    view.add(UserInfoDatasetSnowFlakeWithSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "snowflakeTable": {
                "db": {
                    "snowflake": {
                        "account": "nhb38793.us-west-2.snowflakecomputing.com",
                        "usernameSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["us-west-2", "username"],
                        },
                        "passwordSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["us-west-2", "password"],
                        },
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
        "dataset": "UserInfoDatasetSnowFlakeWithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
            "usernameSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["us-west-2", "username"],
            },
            "passwordSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["us-west-2", "password"],
            },
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


def test_multiple_sources_bigquery():
    @meta(owner="test@test.com")
    @source(
        bigquery.table("users_bq", cursor="added_on"),
        every="1h",
        disorder="2h",
        cdc="upsert",
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
                        "datasetId": "movie_tags",
                        "serviceAccountKey": '{"type": "service_account", "project_id": "fake-project-356105", '
                        '"client_email": '
                        '"randomstring@fake-project-356105.iam.gserviceaccount.com", '
                        '"client_id": "103688493243243272951", "auth_uri": '
                        '"https://accounts.google.com/o/oauth2/auth", "token_uri": '
                        '"https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": '
                        '"https://www.googleapis.com/oauth2/v1/certs"}',
                        "projectId": "gold-cocoa-356105",
                    },
                },
                "tableName": "users_bq",
            }
        },
        "dataset": "UserInfoDatasetBigQuery",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
            "datasetId": "movie_tags",
            "serviceAccountKey": '{"type": "service_account", "project_id": "fake-project-356105", "client_email": '
            '"randomstring@fake-project-356105.iam.gserviceaccount.com", "client_id": '
            '"103688493243243272951", "auth_uri": "https://accounts.google.com/o/oauth2/auth", '
            '"token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": '
            '"https://www.googleapis.com/oauth2/v1/certs"}',
            "projectId": "gold-cocoa-356105",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        bigquery_with_secret.table("users_bq", cursor="added_on"),
        every="1h",
        disorder="2h",
        cdc="upsert",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetBigQueryWithSecret:
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
    view.add(UserInfoDatasetBigQueryWithSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "bigqueryTable": {
                "db": {
                    "name": "bq_movie_tags",
                    "bigquery": {
                        "datasetId": "movie_tags",
                        "serviceAccountKeySecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["bigquery_service_account_key"],
                        },
                        "projectId": "gold-cocoa-356105",
                    },
                },
                "tableName": "users_bq",
            }
        },
        "dataset": "UserInfoDatasetBigQueryWithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
            "datasetId": "movie_tags",
            "serviceAccountKeySecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["bigquery_service_account_key"],
            },
            "projectId": "gold-cocoa-356105",
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_kafka():
    @meta(owner="test@test.com")
    @source(
        kafka.topic("test_topic"),
        disorder="14d",
        cdc="upsert",
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
        "cdc": "Upsert",
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

    protobuf = Protobuf(
        registry="confluent",
        url="http://localhost:8000",
        username="user",
        password="pwd",
    )

    @meta(owner="test@test.com")
    @source(
        kafka.topic(
            "test_topic",
            format=protobuf,
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
                    "protobuf": {
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
        kafka_with_secret.topic(
            "test_topic",
            format=protobuf,
        ),
        cdc="debezium",
        disorder="14d",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetKafkaWithSecret:
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
    view.add(UserInfoDatasetKafkaWithSecret)
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
                        "saslUsernameSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["sasl_plain_username"],
                        },
                        "saslPasswordSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["sasl_plain_password"],
                        },
                    },
                },
                "topic": "test_topic",
                "format": {
                    "protobuf": {
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
        "dataset": "UserInfoDatasetKafkaWithSecret",
        "dsVersion": 1,
        "disorder": "1209600s",
        "cdc": "Debezium",
        "startingFrom": "2021-08-10T00:00:00Z",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )

    avro = Avro(
        registry="confluent",
        url="http://localhost:8000",
        username=aws_secret["avro_username"],
        password=aws_secret["avro_password"],
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
                                "basic": {
                                    "usernameSecret": {
                                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                        "path": ["avro_username"],
                                    },
                                    "passwordSecret": {
                                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                        "path": ["avro_password"],
                                    },
                                }
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

    protobuf = Protobuf(
        registry="confluent",
        url="http://localhost:8000",
        username=aws_secret["protobuf_username"],
        password=aws_secret["protobuf_password"],
    )

    @meta(owner="test@test.com")
    @source(
        kafka.topic(
            "test_topic",
            format=protobuf,
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
                    "protobuf": {
                        "schemaRegistry": {
                            "url": "http://localhost:8000",
                            "auth": {
                                "basic": {
                                    "usernameSecret": {
                                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                        "path": ["protobuf_username"],
                                    },
                                    "passwordSecret": {
                                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                        "path": ["protobuf_password"],
                                    },
                                }
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

    avro = Avro(
        registry="confluent",
        url="http://localhost:8000",
        token=aws_secret["avro_token"],
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
                                "token": {
                                    "tokenSecret": {
                                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                        "path": ["avro_token"],
                                    }
                                }
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

    protobuf = Protobuf(
        registry="confluent",
        url="http://localhost:8000",
        token=aws_secret["protobuf_token"],
    )

    @meta(owner="test@test.com")
    @source(
        kafka.topic(
            "test_topic",
            format=protobuf,
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
                    "protobuf": {
                        "schemaRegistry": {
                            "url": "http://localhost:8000",
                            "auth": {
                                "token": {
                                    "tokenSecret": {
                                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                                        "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                                        "path": ["protobuf_token"],
                                    }
                                }
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


def test_multiple_sources_s3():
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
        cdc="native",
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
        "cdc": "Native",
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
        s3_with_secret.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
            presorted=True,
            format="delta",
        ),
        every="1h",
        disorder="2d",
        cdc="native",
        since=datetime.strptime("2021-08-10T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        until=datetime.strptime("2022-02-28T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    )
    @dataset
    class UserInfoDatasetS3WithSecret:
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
    view.add(UserInfoDatasetS3WithSecret)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    assert len(sync_request.sources) == 1
    assert len(sync_request.extdbs) == 1

    # s3 source with secret
    source_request = sync_request.sources[0]
    s = {
        "table": {
            "s3Table": {
                "bucket": "all_ratings",
                "pathPrefix": "prod/apac/",
                "pathSuffix": "",
                "format": "delta",
                "preSorted": True,
                "db": {
                    "name": "ratings_source",
                    "s3": {
                        "awsSecretAccessKeySecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["s3_aws_secret_access_key"],
                        },
                        "awsAccessKeyIdSecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["s3_aws_access_key_id"],
                        },
                    },
                },
            }
        },
        "dataset": "UserInfoDatasetS3WithSecret",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Native",
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
            "awsSecretAccessKeySecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["s3_aws_secret_access_key"],
            },
            "awsAccessKeyIdSecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["s3_aws_access_key_id"],
            },
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_pubsub():
    @meta(owner="test@test.com")
    @source(
        pubsub.topic("test_topic", format="json"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetPubSub:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # pubsub source
    view = InternalTestClient()
    view.add(UserInfoDatasetPubSub)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]

    s = {
        "table": {
            "pubsubTopic": {
                "db": {
                    "pubsub": {
                        "projectId": "test_project",
                        "serviceAccountKey": '{"type": "service_account", "project_id": "fake-project-356105", '
                        '"client_email": '
                        '"randomstring@fake-project-356105.iam.gserviceaccount.com", "client_id": '
                        '"103688493243243272951", "auth_uri": '
                        '"https://accounts.google.com/o/oauth2/auth", "token_uri": '
                        '"https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": '
                        '"https://www.googleapis.com/oauth2/v1/certs"}',
                    },
                    "name": "pubsub_src",
                },
                "topicId": "test_topic",
                "format": {
                    "json": {},
                },
            }
        },
        "dataset": "UserInfoDatasetPubSub",
        "dsVersion": 1,
        "cdc": "Upsert",
        "disorder": "1209600s",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "pubsub_src",
        "pubsub": {
            "projectId": "test_project",
            "serviceAccountKey": '{"type": "service_account", "project_id": "fake-project-356105", '
            '"client_email": '
            '"randomstring@fake-project-356105.iam.gserviceaccount.com", "client_id": '
            '"103688493243243272951", "auth_uri": '
            '"https://accounts.google.com/o/oauth2/auth", "token_uri": '
            '"https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": '
            '"https://www.googleapis.com/oauth2/v1/certs"}',
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )

    @meta(owner="test@test.com")
    @source(
        pubsub_with_secret.topic("test_topic", format="json"),
        disorder="14d",
        cdc="upsert",
        every="1h",
    )
    @dataset
    class UserInfoDatasetPubSubWithSecret:
        user_id: int = field(key=True)
        name: str
        gender: str
        # Users date of birth
        dob: str
        age: int
        account_creation_date: datetime
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    # pubsub source
    view = InternalTestClient()
    view.add(UserInfoDatasetPubSubWithSecret)
    sync_request = view._get_sync_request_proto()
    source_request = sync_request.sources[0]

    s = {
        "table": {
            "pubsubTopic": {
                "db": {
                    "pubsub": {
                        "projectId": "test_project",
                        "serviceAccountKeySecret": {
                            "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                            "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                            "path": ["pubsub_service_account_key"],
                        },
                    },
                    "name": "pubsub_src",
                },
                "topicId": "test_topic",
                "format": {
                    "json": {},
                },
            }
        },
        "dataset": "UserInfoDatasetPubSubWithSecret",
        "dsVersion": 1,
        "cdc": "Upsert",
        "disorder": "1209600s",
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
    extdb_request = sync_request.extdbs[0]
    e = {
        "name": "pubsub_src",
        "pubsub": {
            "projectId": "test_project",
            "serviceAccountKeySecret": {
                "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:fennel-test-secret-1",
                "roleArn": "arn:aws:iam::123456789012:role/fennel-test-role",
                "path": ["pubsub_service_account_key"],
            },
        },
    }
    expected_extdb_request = ParseDict(e, connector_proto.ExtDatabase())
    assert extdb_request == expected_extdb_request, error_message(
        extdb_request, expected_extdb_request
    )


def test_multiple_sources_kinesis():
    @meta(owner="test@test.com")
    @source(
        kinesis.stream("test_stream", init_position="latest", format="json"),
        disorder="14d",
        cdc="upsert",
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
        "cdc": "Upsert",
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
        cdc="upsert",
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
        cdc="upsert",
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
        "cdc": "Upsert",
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
        "cdc": "Upsert",
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
            format=CSV(delimiter=""),
        ),
        disorder="14d",
        cdc="upsert",
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
                "format": "csv",
                "delimiter": "\x01",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
            format=CSV(delimiter="\x01"),
        ),
        disorder="14d",
        cdc="upsert",
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
                "delimiter": "",
                "format": "csv",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
            format=CSV(
                delimiter="\x01",
                headers=["user_id", "timestamp", "a", "b", "c"],
            ),
        ),
        disorder="14d",
        cdc="upsert",
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
                "delimiter": "\x01",
                "headers": ["user_id", "timestamp", "a", "b", "c"],
                "format": "csv",
                "spread": "21600s",
                "db": {"s3": {}, "name": "s3_test"},
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "cdc": "Upsert",
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
        cdc="upsert",
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
        "cdc": "Upsert",
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


def test_valid_preproc_value():
    # Preproc value of type A[B][C] can be set for data in JSON and Protobuf formats
    source(
        s3.bucket(
            bucket_name="all_ratings", prefix="prod/apac/", format="json"
        ),
        every="1h",
        disorder="14d",
        cdc="native",
        preproc={"C": ref("A[B][C]"), "D": "A[B][C]"},
    )

    source(
        s3.bucket(
            bucket_name="all_ratings", prefix="prod/apac/", format="parquet"
        ),
        every="1h",
        disorder="14d",
        cdc="append",
        preproc={"C": "A[B][C]", "D": "A[B][C]"},
    )

    source(
        kafka.topic(topic="topic", format="Avro"),
        every="1h",
        disorder="14d",
        cdc="debezium",
        preproc={"C": "A[B][C]", "D": "A[B][C]"},
    )
    source(
        kafka.topic(topic="topic"),
        every="1h",
        disorder="14d",
        cdc="debezium",
        preproc={"C": ref("A[B][C]"), "D": "A[B][C]"},
    )
    source(
        kafka.topic(topic="topic", format="json"),
        every="1h",
        disorder="14d",
        cdc="debezium",
        preproc={"C": ref("A[B][C]"), "D": "A[B][D]"},
    )

    protobuf = Protobuf(
        registry="confluent",
        url="http://localhost:8000",
        username="user",
        password="pwd",
    )
    source(
        kafka.topic(topic="topic", format=protobuf),
        every="1h",
        disorder="14d",
        cdc="debezium",
        preproc={"C": ref("A[B][C]"), "D": "A[B][C]"},
    )


def test_filter_preproc():
    if sys.version_info >= (3, 10):

        @source(
            mysql.table(
                "users",
                cursor="added_on",
            ),
            every="1h",
            disorder="20h",
            bounded=True,
            idleness="1h",
            cdc="upsert",
            where=lambda df: df["user_id"] == 1,
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

        assert len(sync_request.sources) == 1
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
                    "tableName": "users",
                }
            },
            "dataset": "UserInfoDataset",
            "dsVersion": 1,
            "every": "3600s",
            "cursor": "added_on",
            "disorder": "72000s",
            "timestampField": "timestamp",
            "cdc": "Upsert",
            "bounded": True,
            "idleness": "3600s",
            "filter": {
                "entryPoint": "UserInfoDataset_wrapper_2e6e95b302_filter",
                "sourceCode": 'lambda df: df["user_id"] == 1',
                "coreCode": 'lambda df: df["user_id"] == 1',
                "generatedCode": '\n\n@meta(owner="test@test.com")\n@dataset\nclass UserInfoDataset:\n    user_id: int = field(key=True)\n    name: str\n    gender: str\n    # Users date of birth\n    dob: str\n    age: int\n    account_creation_date: datetime\n    country: Optional[str]\n    timestamp: datetime = field(timestamp=True)\n\n\n    @classmethod\n    def wrapper_2e6e95b302(cls, *args, **kwargs):\n        _fennel_internal = lambda df: df["user_id"] == 1\n        return _fennel_internal(*args, **kwargs)\n\n\ndef UserInfoDataset_wrapper_2e6e95b302(*args, **kwargs):\n    _fennel_internal = UserInfoDataset.__fennel_original_cls__\n    return getattr(_fennel_internal, "wrapper_2e6e95b302")(*args, **kwargs)\n\ndef UserInfoDataset_wrapper_2e6e95b302_filter(df: pd.DataFrame) -> pd.DataFrame:\n    return df[UserInfoDataset_wrapper_2e6e95b302(df)]\n    ',
                "imports": "__fennel_gen_code__=True\nimport pandas as pd\nimport numpy as np\nimport json\nimport os\nimport sys\nfrom datetime import datetime, date\nimport time\nimport random\nimport math\nimport re\nfrom enum import Enum\nfrom typing import *\nfrom collections import defaultdict\nfrom fennel.connectors.connectors import *\nfrom fennel.datasets import *\nfrom fennel.featuresets import *\nfrom fennel.featuresets import feature\nfrom fennel.featuresets import feature as F\nfrom fennel.lib.expectations import *\nfrom fennel.internal_lib.schema import *\nfrom fennel.internal_lib.utils import *\nfrom fennel.lib.params import *\nfrom fennel.dtypes.dtypes import *\nfrom fennel.datasets.aggregate import *\nfrom fennel.lib.includes import includes\nfrom fennel.lib.metadata import meta\nfrom fennel.lib import secrets, bucketize\nfrom fennel.datasets.datasets import dataset_lookup\nfrom fennel.expr import col, lit, when\n",
            },
        }
        expected_source_request = ParseDict(s, connector_proto.Source())
        assert source_request == expected_source_request, error_message(
            source_request, expected_source_request
        )


def test_assign_python_preproc():
    if sys.version_info >= (3, 10):

        @source(
            mysql.table(
                "users",
                cursor="added_on",
            ),
            every="1h",
            disorder="20h",
            bounded=True,
            idleness="1h",
            cdc="upsert",
            preproc={"age": eval(lambda x: pd.to_numeric(x["age_str"]))},
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
            age_str: str
            timestamp: datetime = field(timestamp=True)

        view = InternalTestClient()
        view.add(UserInfoDataset)
        sync_request = view._get_sync_request_proto()

        assert len(sync_request.sources) == 1
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
                    "tableName": "users",
                }
            },
            "dataset": "UserInfoDataset",
            "dsVersion": 1,
            "every": "3600s",
            "cursor": "added_on",
            "disorder": "72000s",
            "timestampField": "timestamp",
            "cdc": "Upsert",
            "bounded": True,
            "idleness": "3600s",
            "preProc": {"age": {"eval": {"pycode": {}}}},
        }
        expected_source_request = ParseDict(s, connector_proto.Source())
        source_request.pre_proc.get("age").eval.pycode.Clear()
        assert source_request == expected_source_request, error_message(
            source_request, expected_source_request
        )


def test_assign_eval_preproc():

    @source(
        mysql.table(
            "users",
            cursor="added_on",
        ),
        every="1h",
        disorder="20h",
        bounded=True,
        idleness="1h",
        cdc="upsert",
        preproc={
            "age": eval(
                (col("val1") * col("val2") + lit(1)),
                schema={"val1": int, "val2": int},
            )
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
        timestamp: datetime = field(timestamp=True)

    view = InternalTestClient()
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()

    assert len(sync_request.sources) == 1
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
                "tableName": "users",
            }
        },
        "dataset": "UserInfoDataset",
        "dsVersion": 1,
        "every": "3600s",
        "cursor": "added_on",
        "disorder": "72000s",
        "timestampField": "timestamp",
        "cdc": "Upsert",
        "bounded": True,
        "idleness": "3600s",
        "preProc": {
            "age": {
                "eval": {
                    "expr": {},
                    "schema": {
                        "fields": [
                            {"name": "val1", "dtype": {"intType": {}}},
                            {"name": "val2", "dtype": {"intType": {}}},
                        ]
                    },
                }
            }
        },
    }
    expected_source_request = ParseDict(s, connector_proto.Source())
    source_request.pre_proc.get("age").eval.expr.Clear()
    assert source_request == expected_source_request, error_message(
        source_request, expected_source_request
    )
