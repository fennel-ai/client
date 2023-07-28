# docsnip imports
from datetime import datetime, timedelta

import pandas as pd
import requests
from typing import Optional

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.featuresets import feature, featureset, extractor
from fennel.lib.aggregate import Count
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
)
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.lib.window import Window
from fennel.sources import source, Postgres, Snowflake, Kafka, Webhook

# /docsnip

# docsnip connectors
postgres = Postgres.get(name="my_rdbms")
warehouse = Snowflake.get(name="my_warehouse")
kafka = Kafka.get(name="my_kafka")


# /docsnip


# docsnip datasets
@dataset
@source(postgres.table("product_info", cursor="last_modified"), every="1m")
@meta(owner="chris@fennel.ai", tags=["PII"])
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


# ingesting realtime data from Kafka works exactly the same way
@meta(owner="eva@fennel.ai")
@source(kafka.topic("orders"), lateness="1h")
@dataset
class Order:
    uid: int
    product_id: int
    timestamp: datetime


# /docsnip


# docsnip pipelines
@meta(owner="mark@fennel.ai")
@dataset
class UserSellerOrders:
    uid: int = field(key=True)
    seller_id: int = field(key=True)
    num_orders_1d: int
    num_orders_1w: int
    timestamp: datetime

    @pipeline(version=1)
    @inputs(Order, Product)
    def my_pipeline(cls, orders: Dataset, products: Dataset):
        orders = orders.join(products, how="left", on=["product_id"])
        orders = orders.transform(
            lambda df: df[["uid", "seller_id", "timestamp"]].fillna(0),
            schema={
                "uid": int,
                "seller_id": int,
                "timestamp": datetime,
            },
        )

        return orders.groupby("uid", "seller_id").aggregate(
            [
                Count(window=Window("1d"), into_field="num_orders_1d"),
                Count(window=Window("1w"), into_field="num_orders_1w"),
            ]
        )


# /docsnip


# docsnip features
@meta(owner="nikhil@fennel.ai")
@featureset
class UserSellerFeatures:
    uid: int = feature(id=1)
    seller_id: int = feature(id=2)
    num_orders_1d: int = feature(id=3)
    num_orders_1w: int = feature(id=4)

    @extractor(depends_on=[UserSellerOrders])
    @inputs(uid, seller_id)
    @outputs(num_orders_1d, num_orders_1w)
    def myextractor(cls, ts: pd.Series, uids: pd.Series, sellers: pd.Series):
        df, found = UserSellerOrders.lookup(ts, seller_id=sellers, uid=uids)
        df = df.fillna(0)
        df["num_orders_1d"] = df["num_orders_1d"].astype(int)
        df["num_orders_1w"] = df["num_orders_1w"].astype(int)
        return df[["num_orders_1d", "num_orders_1w"]]


# /docsnip


# docsnip sync
from fennel.test_lib import MockClient

webhook = Webhook(name="fennel_webhook")

# client = Client('<FENNEL SERVER URL>') # uncomment this line to use a real Fennel server
client = MockClient()  # comment this line to use a real Fennel server
fake_Product = Product.with_source(webhook.endpoint("Product"))
fake_Order = Order.with_source(webhook.endpoint("Order"))
client.sync(
    datasets=[fake_Order, fake_Product, UserSellerOrders],
    featuresets=[UserSellerFeatures],
)

now = datetime.now()
# create some product data
columns = ["product_id", "seller_id", "price", "desc", "last_modified"]
data = [
    [1, 1, 10.0, "product 1", now],
    [2, 2, 20.0, "product 2", now],
    [3, 1, 30.0, "product 3", now],
]
df = pd.DataFrame(data, columns=columns)
response = client.log("fennel_webhook", "Product", df)
assert response.status_code == requests.codes.OK, response.json()

columns = ["uid", "product_id", "timestamp"]
data = [[1, 1, now], [1, 2, now], [1, 3, now]]
df = pd.DataFrame(data, columns=columns)
response = client.log("fennel_webhook", "Order", df)
assert response.status_code == requests.codes.OK, response.json()
# /docsnip

# docsnip query
feature_df = client.extract_features(
    output_feature_list=[
        UserSellerFeatures.num_orders_1d,
        UserSellerFeatures.num_orders_1w,
    ],
    input_feature_list=[
        UserSellerFeatures.uid,
        UserSellerFeatures.seller_id,
    ],
    input_dataframe=pd.DataFrame(
        {
            "UserSellerFeatures.uid": [1, 1],
            "UserSellerFeatures.seller_id": [1, 2],
        }
    ),
)
assert feature_df.columns.tolist() == [
    "UserSellerFeatures.num_orders_1d",
    "UserSellerFeatures.num_orders_1w",
]
assert feature_df["UserSellerFeatures.num_orders_1d"].tolist() == [2, 1]
assert feature_df["UserSellerFeatures.num_orders_1w"].tolist() == [2, 1]
# /docsnip

# docsnip historical
feature_df = client.extract_historical_features(
    output_feature_list=[
        UserSellerFeatures.num_orders_1d,
        UserSellerFeatures.num_orders_1w,
    ],
    input_feature_list=[
        UserSellerFeatures.uid,
        UserSellerFeatures.seller_id,
    ],
    timestamp_column="timestamps",
    format="pandas",
    input_dataframe=pd.DataFrame(
        {
            "UserSellerFeatures.uid": [1, 1, 1, 1],
            "UserSellerFeatures.seller_id": [1, 2, 1, 2],
            "timestamps": [
                now,
                now,
                now - timedelta(days=1),
                now - timedelta(days=1),
            ],
        }
    ),
)
assert feature_df.columns.tolist() == [
    "UserSellerFeatures.num_orders_1d",
    "UserSellerFeatures.num_orders_1w",
    "timestamps",
]
assert feature_df["UserSellerFeatures.num_orders_1d"].tolist() == [2, 1, 0, 0]
assert feature_df["UserSellerFeatures.num_orders_1w"].tolist() == [2, 1, 0, 0]
# /docsnip
