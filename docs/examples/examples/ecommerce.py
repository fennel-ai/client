# docsnip imports
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from fennel.connectors import Postgres, source, Webhook
from fennel.datasets import dataset, pipeline, field, Dataset, Count
from fennel.dtypes import Continuous
from fennel.featuresets import featureset, extractor
from fennel.lib import meta, inputs, outputs
from fennel.testing import mock

# /docsnip

# docsnip connector
postgres = Postgres(
    name="my-postgres",
    host="somedb",
    db_name="mydb",
    username="myuser",
    password="mypassword",
)


# /docsnip


# docsnip datasets
@source(
    postgres.table("orders", cursor="timestamp"),
    every="1m",
    disorder="1d",
    cdc="append",
    env="prod",
)
@source(
    Webhook(name="fennel_webhook").endpoint("Order"),
    disorder="14d",
    cdc="append",
    env="dev",
)
@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class Order:
    uid: int
    product_id: int
    seller_id: int
    timestamp: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@dataset(index=True)
class UserSellerOrders:
    uid: int = field(key=True)
    seller_id: int = field(key=True)
    num_orders_1d: int
    num_orders_1w: int
    timestamp: datetime

    @pipeline
    @inputs(Order)
    def my_pipeline(cls, orders: Dataset):
        return orders.groupby("uid", "seller_id").aggregate(
            num_orders_1d=Count(window=Continuous("1d")),
            num_orders_1w=Count(window=Continuous("1w")),
        )


# /docsnip


# docsnip featuresets
@meta(owner="feed-ranking-team@fennel.ai")
@featureset
class UserSeller:
    uid: int
    seller_id: int
    num_orders_1d: int
    num_orders_1w: int

    @extractor(deps=[UserSellerOrders])
    @inputs("uid", "seller_id")
    @outputs("num_orders_1d", "num_orders_1w")
    def myextractor(cls, ts: pd.Series, uids: pd.Series, sellers: pd.Series):
        df, found = UserSellerOrders.lookup(ts, uid=uids, seller_id=sellers)
        df = df.fillna(0)
        df["num_orders_1d"] = df["num_orders_1d"].astype(int)
        df["num_orders_1w"] = df["num_orders_1w"].astype(int)
        return df[["num_orders_1d", "num_orders_1w"]]


# /docsnip


# We can write a unit test to verify that the feature is working as expected
# docsnip test


class TestUserLivestreamFeatures(unittest.TestCase):
    @mock
    def test_feature(self, client):
        client.commit(
            message="some commit message",
            datasets=[Order, UserSellerOrders],
            featuresets=[UserSeller],
            env="dev",
        )
        columns = ["uid", "product_id", "seller_id", "timestamp"]
        now = datetime.now(timezone.utc)
        data = [
            [1, 1, 1, now - timedelta(days=8)],
            [1, 2, 1, now - timedelta(days=6)],
            [1, 3, 1, now - timedelta(hours=3)],
            [1, 312, 2, now - timedelta(hours=4)],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "Order", df)
        assert response.status_code == requests.codes.OK, response.json()

        feature_df = client.query(
            outputs=[
                "UserSeller.num_orders_1d",
                "UserSeller.num_orders_1w",
            ],
            inputs=[
                "UserSeller.uid",
                "UserSeller.seller_id",
            ],
            input_dataframe=pd.DataFrame(
                [
                    {"UserSeller.uid": 1, "UserSeller.seller_id": 1},
                    {"UserSeller.uid": 1, "UserSeller.seller_id": 2},
                    {"UserSeller.uid": 2, "UserSeller.seller_id": 3},
                ]
            ),
        )
        self.assertEqual(feature_df.shape, (3, 2))
        self.assertEqual(
            feature_df.columns.tolist(),
            ["UserSeller.num_orders_1d", "UserSeller.num_orders_1w"],
        )
        self.assertEqual(
            feature_df["UserSeller.num_orders_1d"].tolist(), [1, 1, 0]
        )
        self.assertEqual(
            feature_df["UserSeller.num_orders_1w"].tolist(), [2, 1, 0]
        )


# /docsnip
