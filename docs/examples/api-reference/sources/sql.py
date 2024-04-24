from datetime import datetime
import os

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_mysql_basic(client):
    os.environ["MYSQL_USERNAME"] = "some-name"
    os.environ["MYSQL_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip mysql_source
    from fennel.connectors import source, MySQL
    from fennel.datasets import dataset, field

    # docsnip-highlight start
    mysql = MySQL(
        name="my_mysql",
        host="my-favourite-mysql.us-west-2.rds.amazonaws.com",
        port=3306,  # could be omitted, defaults to 3306
        db_name=os.environ["DB_NAME"],
        username=os.environ["MYSQL_USERNAME"],
        password=os.environ["MYSQL_PASSWORD"],
        jdbc_params="enabledTLSProtocols=TLSv1.2",
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    table = mysql.table("user", cursor="updated_at")

    # docsnip-highlight next-line
    @source(table, disorder="14d", cdc="upsert", every="1m")
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        created_at: datetime
        updated_at: datetime = field(timestamp=True)

    # /docsnip

    client.commit(message="some commit msg", datasets=[User])


@mock
def test_postgres_basic(client):
    os.environ["POSTGRES_USERNAME"] = "some-name"
    os.environ["POSTGRES_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip postgres_source
    from fennel.connectors import source, Postgres
    from fennel.datasets import dataset, field

    # docsnip-highlight start
    postgres = Postgres(
        name="my_postgres",
        host="my-favourite-pg.us-west-2.rds.amazonaws.com",
        port=5432,  # could be omitted, defaults to 5432
        db_name=os.environ["DB_NAME"],
        username=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        jdbc_params="enabledTLSProtocols=TLSv1.2",
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    table = postgres.table("user", cursor="updated_at")

    # docsnip-highlight next-line
    @source(table, disorder="14d", cdc="upsert", every="1m")
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        created_at: datetime
        updated_at: datetime = field(timestamp=True)

    # /docsnip
    client.commit(message="some commit msg", datasets=[User])


@mock
def test_snowflake_basic(client):
    os.environ["SNOWFLAKE_USERNAME"] = "some-name"
    os.environ["SNOWFLAKE_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip snowflake_source
    from fennel.connectors import source, Snowflake
    from fennel.datasets import dataset

    # docsnip-highlight start
    snowflake = Snowflake(
        name="my_snowflake",
        account="VPECCVJ-MUB03765",
        warehouse="TEST",
        db_name=os.environ["DB_NAME"],
        schema="PUBLIC",
        role="ACCOUNTADMIN",
        username=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    table = snowflake.table("User", cursor="timestamp")

    # docsnip-highlight next-line
    @source(table, disorder="14d", cdc="append")
    @dataset
    class UserClick:
        uid: int
        ad_id: int
        timestamp: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[UserClick])


@mock
def test_bigquery_basic(client):
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip bigquery_source
    from fennel.connectors import source, BigQuery
    from fennel.datasets import dataset

    # docsnip-highlight start
    bq = BigQuery(
        name="my_bigquery",
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
    # docsnip-highlight end

    # docsnip-highlight next-line
    table = bq.table("user", cursor="timestamp")

    # docsnip-highlight next-line
    @source(table, disorder="14d", cdc="append")
    @dataset
    class UserClick:
        uid: int
        ad_id: int
        timestamp: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[UserClick])


@mock
def test_redshift_basic(client):
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip redshift_source
    from fennel.connectors import source, Redshift
    from fennel.datasets import dataset

    # docsnip-highlight start
    redshift = Redshift(
        name="my_redshift",
        s3_access_role_arn="arn:aws:iam::123:role/Redshift",
        db_name=os.environ["DB_NAME"],
        host="test-workgroup.1234.us-west-2.redshift-serverless.amazonaws.com",
        port=5439,  # could be omitted, defaults to 5439
        schema="public",
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    table = redshift.table("user", cursor="timestamp")

    # docsnip-highlight next-line
    @source(table, disorder="14d", cdc="append")
    @dataset
    class UserClick:
        uid: int
        ad_id: int
        timestamp: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[UserClick])


@mock
def test_mongo_basic(client):
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip mongo_source
    from fennel.connectors import source, Mongo
    from fennel.datasets import dataset

    # docsnip-highlight start
    mongo = Mongo(
        name="mongo_src",
        host="atlascluster.ushabcd.mongodb.net",
        db_name="mongo",
        username="username",
        password="password",
    )
    # docsnip-highlight end

    # docsnip-highlight next-line
    collection = mongo.collection("user", cursor="timestamp")

    @source(collection, disorder="14d", cdc="append")  # docsnip-highlight
    @dataset
    class UserClick:
        uid: int
        ad_id: int
        timestamp: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[UserClick])


@mock
def test_pubsub_basic(client):
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip pubsub_source
    from fennel.connectors import source, PubSub
    from fennel.datasets import dataset, field

    # docsnip-highlight start
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
    # docsnip-highlight end

    # docsnip-highlight next-line
    collection = pubsub.topic("test_topic")

    @source(collection, disorder="2d", cdc="upsert")  # docsnip-highlight
    @dataset
    class UserClick:
        uid: int = field(key=True)
        ad_id: int
        timestamp: datetime

    # /docsnip
    client.commit(message="some commit msg", datasets=[UserClick])
