from datetime import datetime

import pandas as pd

from fennel import sources
from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.sources import source
from fennel.test_lib import MockClient

# docsnip mysql_source
mysql = sources.MySQL(
    name="py_mysql_src",
    host="my-favourite-mysql.us-west-2.rds.amazonaws.com",
    port=3306,
    db_name="some_database_name",
    username="admin",
    password="password",
    jdbc_params="enabledTLSProtocols=TLSv1.2",
)


@source(mysql.table("user", cursor="update_time"), every="1m")
@dataset
class UserMysqlSourcedDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip


# docsnip postgres_source
postgres = sources.Postgres(
    name="py_psql_src",
    host="my-favourite-postgres.us-west-2.rds.amazonaws.com",
    db_name="some_database_name",
    username="admin",
    password="password",
)


@source(postgres.table("user", cursor="update_time"), every="1m")
@dataset
class UserPostgresSourcedDataset:
    uid: int
    timestamp: datetime
    ...


# /docsnip

# docsnip s3_source
s3 = sources.S3(
    name="ratings_source",
    aws_access_key_id="<SOME_ACCESS_KEY>",
    aws_secret_access_key="<SOME_SECRET_ACCESS_KEY>",
)


@source(s3.bucket("engagement", prefix="notion"), every="30m")
@meta(owner="abc@email.com")
@dataset
class UserS3SourcedDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip

# docsnip snowflake_source
sf_src = sources.Snowflake(
    name="snowflake_src",
    account="nhb38793.us-west-2.snowflakecomputing.com",
    warehouse="TEST",
    schema="PUBLIC",
    db_name="TEST_DB",
    src_schema="PUBLIC",
    role="ACCOUNTADMIN",
    username="<username>",
    password="<password>",
)
# /docsnip


client = MockClient()
df = pd.DataFrame(
    {
        "uid": [1, 2, 3],
        "email": ["a@gmail.com", "b@gmail.com", "c@gmail.com"],
        "timestamp": [datetime.now(), datetime.now(), datetime.now()],
    }
)

# docsnip webhook_source
webhook = sources.Webhook(name="fennel_webhook")


@source(webhook.endpoint("UserDataset"))
@meta(owner="abc@email.com")
@dataset
class UserDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


client.log("fennel_webhook", "UserDataset", df)
# /docsnip


# docsnip s3_hudi_source
s3 = sources.S3(
    name="ratings_source",
    aws_access_key_id="<SOME_ACCESS_KEY>",
    aws_secret_access_key="<SOME_SECRET_ACCESS_KEY>",
)


@source(s3.bucket("engagement", prefix="notion", format="hudi"), every="30m")
@meta(owner="abc@email.com")
@dataset
class UserHudiSourcedDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip

# docsnip kafka_source
kafka = sources.Kafka(
    name="kafka_src",
    bootstrap_servers="localhost:9092",
    security_protocol="PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="test",
    sasl_plain_password="test",
    verify_cert=False,
)


@source(kafka.topic("user"))
@meta(owner="abc@email.com")
@dataset
class UserKafkaSourcedDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip

# docsnip kinesis_source
from fennel.sources.kinesis import at_timestamp

kinesis = sources.Kinesis(
    name="kinesis_src",
    role_arn="<SOME_ROLE_ARN>",
)
stream = kinesis.stream(
    stream_arn="<SOME_STREAM_ARN>",
    # Start ingesting from Nov 5, 2023
    init_position=at_timestamp(datetime(2023, 11, 5)),
)


@source(stream)
@meta(owner="abc@email.com")
@dataset
class UserKinesisSourcedDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip

# docsnip kinesis_source_latest
kinesis = sources.Kinesis(
    name="kinesis_src",
    role_arn="<SOME_ROLE_ARN>",
)
stream = kinesis.stream(
    stream_arn="<SOME_STREAM_ARN>",
    # Ingest all new records from now
    init_position="latest",
)


@source(stream)
@meta(owner="abc@email.com")
@dataset
class UserKinesisSourcedDataset2:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip

# docsnip s3_delta_lake_source
s3 = sources.S3(
    name="ratings_source",
    aws_access_key_id="<SOME_ACCESS_KEY>",
    aws_secret_access_key="<SOME_SECRET_ACCESS_KEY>",
)


@source(s3.bucket("engagement", prefix="notion", format="delta"), every="30m")
@meta(owner="abc@email.com")
@dataset
class UserDeltaLakeSourcedDataset:
    uid: int = field(key=True)
    email: str
    timestamp: datetime
    ...


# /docsnip
