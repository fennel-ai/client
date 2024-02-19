from datetime import datetime
from datetime import timedelta
from typing import Optional

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.datasets import Count
from fennel.lib import meta
from fennel.lib import inputs, outputs
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")


# docsnip filter
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("Action"))
@dataset
class Action:
    uid: int
    action_type: str
    timestamp: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class Likes:
    uid: int
    action_type: str
    timestamp: datetime

    @pipeline
    @inputs(Action)
    def filter_likes(cls, actions: Dataset):
        return actions.filter(lambda df: df["action_type"] == "like")


# /docsnip


@mock
def test_filter(client):
    client.commit(datasets=[Action, Likes])
    data = [
        {"uid": 1, "action_type": "like", "timestamp": datetime(2020, 1, 1)},
        {"uid": 1, "action_type": "comment", "timestamp": datetime(2020, 1, 1)},
        {"uid": 2, "action_type": "like", "timestamp": datetime(2020, 1, 1)},
        {"uid": 2, "action_type": "like", "timestamp": datetime(2020, 1, 1)},
        {"uid": 2, "action_type": "share", "timestamp": datetime(2020, 1, 1)},
    ]
    df = pd.DataFrame(data)
    client.log("fennel_webhook", "Action", df)
    df = client.get_dataset_df("Likes")
    assert df.shape == (3, 3)


# docsnip transform
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("Rating"))
@dataset
class Rating:
    movie: str = field(key=True)
    rating: float
    timestamp: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class RatingRescaled:
    movie: str = field(key=True)
    rescaled: float
    timestamp: datetime

    @pipeline
    @inputs(Rating)
    def pipeline_transform(cls, ratings: Dataset):
        def rescale(df: pd.DataFrame) -> pd.DataFrame:
            df["rescaled"] = df["rating"] / 5
            return df[["movie", "timestamp", "rescaled"]]

        return ratings.transform(
            rescale,
            schema={
                "movie": str,
                "timestamp": datetime,
                "rescaled": float,
            },
        )


# /docsnip


@mock
def test_transform(client):
    client.commit(datasets=[Rating, RatingRescaled])
    data = [
        {"movie": "movie1", "rating": 3.0, "timestamp": datetime(2020, 1, 1)},
        {"movie": "movie2", "rating": 4.0, "timestamp": datetime(2020, 1, 1)},
        {"movie": "movie3", "rating": 5.0, "timestamp": datetime(2020, 1, 1)},
    ]
    df = pd.DataFrame(data)
    client.log("fennel_webhook", "Rating", df)
    df = client.get_dataset_df("RatingRescaled")
    assert df.shape == (3, 3)
    assert df["rescaled"].sum() == 2.4  # 3/5 + 4/5 + 5/5


# docsnip join
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("Product"))
@dataset
class Product:
    pid: int = field(key=True)
    seller_id: int
    creation: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("OrderActivity"))
@dataset
class OrderActivity:
    uid: int
    pid: int
    at: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class UserSellerActivity:
    pid: int
    uid: int
    seller_id: Optional[int]
    at: datetime

    @pipeline
    @inputs(Product, OrderActivity)
    def join_orders(cls, products: Dataset, orders: Dataset) -> Dataset:
        return orders.join(products, how="left", on=["pid"])


# /docsnip


@mock
def test_join(client):
    client.commit(datasets=[Product, OrderActivity, UserSellerActivity])
    data = [
        {"pid": 1, "seller_id": 1, "creation": datetime(2020, 1, 1)},
        {"pid": 2, "seller_id": 2, "creation": datetime(2020, 1, 1)},
        {"pid": 3, "seller_id": 13, "creation": datetime(2020, 1, 1)},
    ]
    df = pd.DataFrame(data)
    client.log("fennel_webhook", "Product", df)
    data = [
        {"uid": 1, "pid": 1, "at": datetime(2020, 1, 1)},
        {"uid": 1, "pid": 2, "at": datetime(2020, 1, 1)},
        {"uid": 2, "pid": 3, "at": datetime(2020, 1, 1)},
    ]
    df = pd.DataFrame(data)
    client.log("fennel_webhook", "OrderActivity", df)
    df = client.get_dataset_df("UserSellerActivity")
    assert df.shape == (3, 4)
    assert df["seller_id"].tolist() == [1, 2, 13]


# docsnip aggregate
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("AdClickStream"))
@dataset
class AdClickStream:
    uid: int
    adid: int
    at: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class UserAdStats:
    uid: int = field(key=True)
    num_clicks: int
    num_clicks_1w: int
    at: datetime

    @pipeline
    @inputs(AdClickStream)
    def aggregate_ad_clicks(cls, ad_clicks: Dataset):
        return ad_clicks.groupby("uid").aggregate(
            [
                Count(window="forever", into_field="num_clicks"),
                Count(window="1w", into_field="num_clicks_1w"),
            ]
        )


# /docsnip

from fennel.featuresets import featureset, feature, extractor


@meta(owner="ml-eng@fennel.ai")
@featureset
class UserAdStatsFeatures:
    uid: int = feature(id=1)
    num_clicks: int = feature(id=2)
    num_clicks_1w: int = feature(id=3)

    @extractor(depends_on=[UserAdStats])
    @inputs(uid)
    @outputs(num_clicks, num_clicks_1w)
    def extract(cls, ts, uids):
        df, found = UserAdStats.lookup(ts, uid=uids)
        return df


@mock
def test_aggregate(client):
    client.commit(
        datasets=[AdClickStream, UserAdStats], featuresets=[UserAdStatsFeatures]
    )
    data = [
        {"uid": 1, "adid": 1, "at": datetime(2020, 1, 1)},
        {"uid": 1, "adid": 2, "at": datetime(2020, 1, 1)},
        {"uid": 2, "adid": 3, "at": datetime(2020, 1, 1)},
        {"uid": 2, "adid": 3, "at": datetime(2020, 1, 10)},
        {"uid": 1, "adid": 3, "at": datetime(2020, 1, 11)},
        {"uid": 1, "adid": 3, "at": datetime(2020, 1, 12)},
        {"uid": 2, "adid": 3, "at": datetime(2020, 1, 13)},
    ]
    df = pd.DataFrame(data)
    client.log("fennel_webhook", "AdClickStream", df)
    dt = datetime(2020, 1, 13)
    yes = dt - timedelta(days=1)
    three_days_ago = dt - timedelta(days=3)
    ts_series = pd.Series([dt, yes, dt, three_days_ago, yes])
    uids = pd.Series([1, 1, 2, 2, 2])
    df = client.query_offline(
        inputs=[UserAdStatsFeatures.uid],
        outputs=[UserAdStatsFeatures],
        input_dataframe=pd.DataFrame(
            {"UserAdStatsFeatures.uid": uids, "timestamps": ts_series}
        ),
        timestamp_column="timestamps",
    )
    assert df["UserAdStatsFeatures.num_clicks"].tolist() == [4, 4, 3, 2, 2]
    assert df["UserAdStatsFeatures.num_clicks_1w"].tolist() == [2, 2, 2, 1, 1]
    assert "timestamps" in df.columns
