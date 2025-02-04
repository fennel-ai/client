# docsnip imports
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from fennel.connectors import source, Postgres, Kafka, Webhook, sink
from fennel.datasets import dataset, pipeline, field, Dataset, Count, Sum
from fennel.dtypes import Continuous
from fennel.featuresets import featureset, extractor, feature as F
from fennel.lib import expectations, expect_column_values_to_be_between
from fennel.lib import inputs, outputs
from fennel.testing import MockClient, log
from fennel.expr import col

__owner__ = "nikhil@fennel.ai"

# /docsnip


# docsnip connectors
postgres = Postgres.get(name="my_rdbms")
kafka = Kafka.get(name="my_kafka")
webhook = Webhook(name="fennel_webhook")


# /docsnip


# docsnip datasets
@source(kafka.topic("orders"), disorder="1h", cdc="append")
@dataset
class Order:
    uid: int
    product_id: int
    timestamp: datetime


# ingesting data from postgres works exactly the same way
table = postgres.table("product", cursor="last_modified")


@source(table, disorder="1d", cdc="upsert", every="1m")
@dataset(index=True)
class Product:
    product_id: int = field(key=True)
    seller_id: int
    price: float
    desc: Optional[str]
    last_modified: datetime = field(timestamp=True)

    # Powerful primitives like data expectations for data hygiene
    @expectations
    def get_expectations(cls):
        return [
            expect_column_values_to_be_between(
                column="price", min_value=1, max_value=1e4, mostly=0.95
            )
        ]


# /docsnip


# docsnip pipelines
@sink(kafka.topic("user_seller_orders"), cdc="debezium")
@dataset(index=True, version=2)
class UserSellerOrders:
    uid: int = field(key=True)
    seller_id: int = field(key=True)
    num_orders_1d: int
    total_cents: int
    timestamp: datetime

    @pipeline
    @inputs(Order, Product)
    def my_pipeline(cls, orders: Dataset, products: Dataset):
        orders = orders.join(products, how="left", on=["product_id"])
        orders = orders.transform(lambda df: df.fillna(0))
        orders = orders.dropnull()
        orders = orders.assign(cents=(col("price") * 100).round().astype(int))
        orders = orders.drop("product_id", "desc", "price")
        return orders.groupby("uid", "seller_id").aggregate(
            num_orders_1d=Count(window=Continuous("1d")),
            total_cents=Sum(of="cents", window=Continuous("forever")),
        )


# /docsnip


# docsnip features
@featureset
class UserSellerFeatures:
    uid: int
    seller_id: int
    num_orders_1d: int = F(UserSellerOrders.num_orders_1d, default=0)
    total_cents: int = F(UserSellerOrders.total_cents, default=0)
    total: float = F(col("total_cents") / 100)


# /docsnip


# docsnip commit
# client = Client('<FENNEL SERVER URL>') # uncomment this to use real Fennel server
client = MockClient()  # comment this line to use a real Fennel server
client.commit(
    message="initial commit",
    datasets=[Order, Product, UserSellerOrders],
    featuresets=[UserSellerFeatures],
)

# /docsnip

# docsnip log_data
# create some product data
now = datetime.now(timezone.utc)
columns = ["product_id", "seller_id", "price", "desc", "last_modified"]
data = [
    [1, 1, 10.0, "product 1", now],
    [2, 2, 20.0, "product 2", now],
    [3, 1, 30.0, "product 3", now],
]
df = pd.DataFrame(data, columns=columns)
log(Product, df)

columns = ["uid", "product_id", "timestamp"]
data = [[1, 1, now], [1, 2, now], [1, 3, now]]
df = pd.DataFrame(data, columns=columns)
log(Order, df)

# /docsnip

# docsnip query
feature_df = client.query(
    outputs=[
        UserSellerFeatures.num_orders_1d,
        UserSellerFeatures.total,
    ],
    inputs=[
        UserSellerFeatures.uid,
        UserSellerFeatures.seller_id,
    ],
    input_dataframe=pd.DataFrame(
        [[1, 1], [1, 2]],
        columns=["UserSellerFeatures.uid", "UserSellerFeatures.seller_id"],
    ),
)

assert feature_df.columns.tolist() == [
    "UserSellerFeatures.num_orders_1d",
    "UserSellerFeatures.total",
]
assert feature_df["UserSellerFeatures.num_orders_1d"].tolist() == [2, 1]
assert feature_df["UserSellerFeatures.total"].tolist() == [40.0, 20.0]
# /docsnip

# docsnip historical
day = timedelta(days=1)

feature_df = client.query_offline(
    outputs=[
        UserSellerFeatures.num_orders_1d,
        UserSellerFeatures.total,
    ],
    inputs=[
        UserSellerFeatures.uid,
        UserSellerFeatures.seller_id,
    ],
    timestamp_column="timestamps",
    input_dataframe=pd.DataFrame(
        [[1, 1, now], [1, 2, now], [1, 1, now - day], [1, 2, now - day]],
        columns=[
            "UserSellerFeatures.uid",
            "UserSellerFeatures.seller_id",
            "timestamps",
        ],
    ),
)

assert feature_df.columns.tolist() == [
    "UserSellerFeatures.num_orders_1d",
    "UserSellerFeatures.total",
    "timestamps",
]
assert feature_df["UserSellerFeatures.num_orders_1d"].tolist() == [2, 1, 0, 0]
assert feature_df["UserSellerFeatures.total"].tolist() == [40.0, 20.0, 0.0, 0.0]
# /docsnip
