import os
from datetime import datetime

from fennel.datasets import dataset
from fennel.lib.metadata import meta

os.environ["POSTGRES_NAME"] = "postgres"
os.environ["POSTGRES_HOST"] = "localhost"
os.environ["DB_NAME"] = "db"
os.environ["USERNAME"] = "username"
os.environ["PASSWORD"] = "password"

# docsnip postgres_source
from fennel.sources import source, Postgres

postgres = Postgres(
    name=os.getenv("POSTGRES_NAME"),
    host=os.getenv("POSTGRES_HOST"),
    db_name=os.getenv("DB_NAME"),
    username=os.getenv("USERNAME"),
    password=os.getenv("PASSWORD"),
)


@meta(owner="data-eng-oncall@fennel.ai")
@source(postgres.table("user", cursor="update_timestamp"), every="1m")
@dataset
class UserLocation:
    uid: int
    timestamp: datetime


# /docsnip
