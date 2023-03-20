# docsnip imports
import unittest
import pandas as pd
import requests
from fennel.datasets import dataset, pipeline, field, Dataset
from datetime import datetime, timedelta
from fennel.lib.metadata import meta
from fennel.lib.aggregate import Count
from fennel.lib.window import Window
from fennel.featuresets import feature, featureset, extractor, depends_on
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import mock_client
# /docsnip

# docsnip connector
from fennel.sources import Postgres, source

postgres = Postgres(
    name="my-postgres",
    host="somedb",
    db_name="mydb",
    username="myuser",
    password="mypassword",
)
# /docsnip

# docsnip definitions
@source(postgres.table("orders", cursor="timestamp"), every="1m", lateness="1d")
@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class Order:
    uid: int
    product_id: int
    seller_id: int
    timestamp: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class UserSellerOrders:
    uid: int = field(key=True)
    seller_id: int = field(key=True)
    num_orders_1d: int
    num_orders_1w: int
    timestamp: datetime

    @pipeline(id=1)
    def my_pipeline(cls, orders: Dataset[Order]):
        return orders.groupby("uid", "seller_id").aggregate(
            [
                Count(window=Window("1d"), into_field="num_orders_1d"),
                Count(window=Window("1w"), into_field="num_orders_1w"),
            ]
        )


@meta(owner="feed-ranking-team@fennel.ai")
@featureset
class UserSeller:
    uid: int = feature(id=1)
    seller_id: int = feature(id=2)
    num_orders_1d: int = feature(id=3)
    num_orders_1w: int = feature(id=4)

    @depends_on(UserSellerOrders)
    @extractor
    def myextractor(
        cls, ts: Series[datetime], uids: Series[uid], sellers: Series[seller_id]
    ) -> DataFrame[num_orders_1d, num_orders_1w]:
        df, found = UserSellerOrders.lookup(ts, uid=uids, seller_id=sellers)
        df = df.fillna(0)
        df["num_orders_1d"] = df["num_orders_1d"].astype(int)
        df["num_orders_1w"] = df["num_orders_1w"].astype(int)
        return df[["num_orders_1d", "num_orders_1w"]]
# /docsnip


# We can write a unit test to verify that the feature is working as expected
# docsnip test
class TestUserLivestreamFeatures(unittest.TestCase):
    @mock_client
    def test_feature(self, client):
        client.sync(
            datasets=[Order, UserSellerOrders], featuresets=[UserSeller]
        )
        columns = ["uid", "product_id", "seller_id", "timestamp"]
        now = datetime.now()
        data = [
            [1, 1, 1, now - timedelta(days=8)],
            [1, 2, 1, now - timedelta(days=6)],
            [1, 3, 1, now - timedelta(hours=3)],
            [1, 312, 2, now - timedelta(hours=4)],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("Order", df)
        assert response.status_code == requests.codes.OK, response.json()

        feature_df = client.extract_features(
            output_feature_list=[
                UserSeller.num_orders_1d,
                UserSeller.num_orders_1w,
            ],
            input_feature_list=[
                UserSeller.uid,
                UserSeller.seller_id,
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
