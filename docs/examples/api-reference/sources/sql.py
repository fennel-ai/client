from datetime import datetime
import os

from fennel.test_lib import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_mysql_basic(client):
    os.environ["MYSQL_USERNAME"] = "some-name"
    os.environ["MYSQL_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip mysql_source
    from fennel.sources import source, MySQL
    from fennel.datasets import dataset, field

    mysql = MySQL(
        name="my_mysql",
        host="my-favourite-mysql.us-west-2.rds.amazonaws.com",
        port=3306,  # could be omitted, defaults to 3306
        db_name=os.environ["DB_NAME"],
        username=os.environ["MYSQL_USERNAME"],
        password=os.environ["MYSQL_PASSWORD"],
        jdbc_params="enabledTLSProtocols=TLSv1.2",
    )

    @source(mysql.table("user", cursor="updated_at"), every="1m")
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        created_at: datetime
        updated_at: datetime = field(timestamp=True)

    # /docsnip

    client.commit(datasets=[User])


@mock
def test_postgres_basic(client):
    os.environ["POSTGRES_USERNAME"] = "some-name"
    os.environ["POSTGRES_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip postgres_source
    from fennel.sources import source, Postgres
    from fennel.datasets import dataset, field

    postgres = Postgres(
        name="my_postgres",
        host="my-favourite-pg.us-west-2.rds.amazonaws.com",
        port=5432,  # could be omitted, defaults to 5432
        db_name=os.environ["DB_NAME"],
        username=os.environ["POSTGRES_USERNAME"],
        password=os.environ["POSTGRES_PASSWORD"],
        jdbc_params="enabledTLSProtocols=TLSv1.2",
    )

    @source(postgres.table("user", cursor="updated_at"), every="1m")
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        created_at: datetime
        updated_at: datetime = field(timestamp=True)

    # /docsnip
    client.commit(datasets=[User])


@mock
def test_snowflake_basic(client):
    os.environ["SNOWFLAKE_USERNAME"] = "some-name"
    os.environ["SNOWFLAKE_PASSWORD"] = "some-password"
    os.environ["DB_NAME"] = "some-db-name"
    # docsnip snowflake_source
    from fennel.sources import source, Snowflake
    from fennel.datasets import dataset, field

    snowflake = Snowflake(
        name="my_snowflake",
        account="VPECCVJ-MUB03765",
        warehouse="TEST",
        db_name=os.environ["DB_NAME"],
        src_schema="PUBLIC",
        role="ACCOUNTADMIN",
        username=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
    )

    @source(snowflake.table("User", cursor="timestamp"))
    @dataset
    class UserClick:
        uid: int
        ad_id: int
        timestamp: datetime

    # /docsnip
    client.commit(datasets=[UserClick])
