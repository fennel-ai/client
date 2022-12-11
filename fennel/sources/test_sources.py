from datetime import datetime
from typing import Optional

from google.protobuf.json_format import ParseDict  # type: ignore

from fennel.datasets import dataset, field
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.metadata import meta
from fennel.sources import source, MySQL, S3, Snowflake, BigQuery, Postgres

# noinspection PyUnresolvedReferences
from fennel.test_lib import *

mysql = MySQL(
    name="mysql",
    host="localhost",
    db_name="test",
    username="root",
    password="root",
)


def test_SimpleSource(grpc_stub):
    @source(
        mysql.table(
            "users",
            cursor_field="added_on",
        ),
        every="1h",
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
    assert len(sync_request.dataset_requests) == 1
    dataset_request = sync_request.dataset_requests[0]
    assert len(dataset_request.input_connectors) == 1
    d = {
        "datasetRequests": [
            {
                "name": "UserInfoDataset",
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"scalarType": "INT"},
                        "isKey": True,
                        "metadata": {},
                    },
                    {
                        "name": "name",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "gender",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "dob",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {"description": "Users date of birth"},
                    },
                    {
                        "name": "age",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "account_creation_date",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                    {
                        "name": "country",
                        "dtype": {"isNullable": True, "scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "isTimestamp": True,
                        "metadata": {},
                    },
                ],
                "inputConnectors": [
                    {
                        "source": {
                            "name": "mysql",
                            "sql": {
                                "sqlType": "MySQL",
                                "host": "localhost",
                                "db": "test",
                                "username": "root",
                                "password": "root",
                                "port": 3306,
                            },
                        },
                        "cursorField": "added_on",
                        "table": "users",
                        "every": "3600000000",
                    }
                ],
                "signature": "b7cb8565c45b59f577d655496226cdae",
                "metadata": {"owner": "test@test.com"},
                "mode": "pandas",
                "retention": "63072000000000",
            }
        ]
    }
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )

    @dataset
    @source(mysql.table("users", cursor_field="added_on"), every="1h")
    @meta(owner="test@test.com")
    class UserInfoDatasetInvertedOrder:
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
    view.add(UserInfoDatasetInvertedOrder)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    dataset_request = sync_request.dataset_requests[0]
    assert len(dataset_request.input_connectors) == 1
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request.dataset_requests[
        0
    ].name = "UserInfoDatasetInvertedOrder"
    expected_sync_request.dataset_requests[
        0
    ].signature = "895490181110ce7573a73c460205d0d8"
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
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


def test_MultipleSources(grpc_stub):
    @meta(owner="test@test.com")
    @source(mysql.table("users_mysql", cursor_field="added_on"), every="1h")
    @source(bigquery.table("users_bq", cursor_field="added_on"), every="1h")
    @source(snowflake.table("users_Sf", cursor_field="added_on"), every="1h")
    @source(
        s3.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
            src_schema={"Name": "string", "Weight": "number", "Age": "integer"},
        ),
        every="1h",
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
    assert len(sync_request.dataset_requests) == 1
    dataset_request = sync_request.dataset_requests[0]
    assert len(dataset_request.input_connectors) == 4


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


def test_ConsoleSource(grpc_stub):
    @source(posgres_console.table("users", cursor_field="added_on"), every="1h")
    @source(mysql_console.table("users", cursor_field="added_on"), every="1h")
    @source(
        snowflake_console.table("users", cursor_field="added_on"), every="1h"
    )
    @source(
        bigquery_console.table("users", cursor_field="added_on"), every="1h"
    )
    @source(
        s3_console.bucket(
            bucket_name="all_ratings",
            prefix="prod/apac/",
            src_schema={"Name": "string", "Weight": "number", "Age": "integer"},
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
    assert len(sync_request.dataset_requests) == 1
    dataset_request = sync_request.dataset_requests[0]
    assert len(dataset_request.input_connectors) == 5
    sync_request.dataset_requests[0].schema = b""
    d = {
        "datasetRequests": [
            {
                "name": "UserInfoDataset",
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"scalarType": "INT"},
                        "isKey": True,
                        "metadata": {},
                    },
                    {
                        "name": "timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "isTimestamp": True,
                        "metadata": {},
                    },
                ],
                "inputConnectors": [
                    {
                        "source": {"name": "s3_test", "s3": {}},
                        "s3Connector": {
                            "bucket": "all_ratings",
                            "pathPrefix": "prod/apac/",
                            "schema": {
                                "Age": "integer",
                                "Weight": "number",
                                "Name": "string",
                            },
                            "delimiter": ",",
                            "format": "csv",
                        },
                        "every": "3600000000",
                    },
                    {
                        "source": {"name": "bigquery_test", "bigquery": {}},
                        "cursorField": "added_on",
                        "table": "users",
                        "every": "3600000000",
                    },
                    {
                        "source": {"name": "snowflake_test", "snowflake": {}},
                        "cursorField": "added_on",
                        "table": "users",
                        "every": "3600000000",
                    },
                    {
                        "source": {
                            "name": "mysql_test",
                            "sql": {"sqlType": "MySQL", "port": 3306},
                        },
                        "cursorField": "added_on",
                        "table": "users",
                        "every": "3600000000",
                    },
                    {
                        "source": {
                            "name": "posgres_test",
                            "sql": {"port": 5432},
                        },
                        "cursorField": "added_on",
                        "table": "users",
                        "every": "3600000000",
                    },
                ],
                "signature": "c0ddc846ab843b58026953df3828cbba",
                "metadata": {
                    "owner": "test@test.com",
                    "tags": ["test", "yolo"],
                },
                "mode": "pandas",
                "retention": "63072000000000",
            }
        ]
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )
