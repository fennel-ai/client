from datetime import datetime

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.datasets import dataset, field, Dataset, pipeline, Count
from fennel.featuresets import featureset, feature, extractor
from fennel.lib import meta, inputs, outputs
from fennel.dtypes import regex, oneof
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")


@meta(owner="data-eng@myspace.com")
@source(webhook.endpoint("UserInfo"), cdc="append", disorder="14d")
@dataset
class UserInfo:
    user_id: str = field(key=True)
    first_name: regex("[A-Z][a-z]+")  # type: ignore
    last_name: str
    gender: oneof(str, ["Female", "Male"])  # type: ignore
    avatar: str
    city: str
    academics: oneof(str, ["undergraduate", "graduate"])  # type: ignore
    timestamp: datetime


@source(webhook.endpoint("PostInfo"), disorder="14d", cdc="append")
@dataset
@meta(owner="data-eng@myspace.com")
class PostInfo:
    title: str
    category: str  # type: ignore
    post_id: int = field(key=True)
    timestamp: datetime


@meta(owner="data-eng@myspace.com")
@dataset
@source(webhook.endpoint("ViewData"), disorder="14d", cdc="append")
class ViewData:
    user_id: str
    post_id: int
    time_stamp: datetime


@meta(owner="ml-eng@myspace.com")
@dataset
class CityInfo:
    city: str = field(key=True)
    gender: oneof(str, ["Female", "Male"]) = field(key=True)  # type: ignore
    count: int
    timestamp: datetime

    @pipeline
    @inputs(UserInfo)
    def count_city_gender(cls, user_info: Dataset):
        return user_info.groupby(["city", "gender"]).aggregate(
            [Count(window="6y 8s", into_field="count")]
        )


@dataset
@meta(owner="ml-eng@myspace.com")
class UserViewsDataset:
    user_id: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @pipeline
    @inputs(ViewData)
    def count_user_views(cls, view_data: Dataset):
        return view_data.groupby("user_id").aggregate(
            [Count(window="6y 8s", into_field="num_views")]
        )


@dataset
@meta(owner="ml-eng@myspace.com")
class UserCategoryDataset:
    user_id: str = field(key=True)
    category: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @pipeline
    @inputs(ViewData, PostInfo)
    def count_user_views(cls, view_data: Dataset, post_info: Dataset):
        post_info_enriched = view_data.join(
            post_info, how="inner", on=["post_id"]
        )
        return post_info_enriched.groupby("user_id", "category").aggregate(
            [Count(window="6y 8s", into_field="num_views")]
        )


# --- Featuresets ---


@meta(owner="feature-team@myspace.com")
@featureset
class Request:
    user_id: str = feature(id=1)
    category: str = feature(id=2)


@meta(owner="feature-team@myspace.com")
@featureset
class UserFeatures:
    num_views: int = feature(id=2)
    num_category_views: int = feature(id=3)
    category_view_ratio: float = feature(id=4)

    @extractor(depends_on=[UserViewsDataset])
    @inputs(Request.user_id)
    @outputs(num_views)
    def extract_user_views(cls, ts: pd.Series, user_ids: pd.Series):
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        views = views.fillna(0)

        return views["num_views"]

    @extractor(depends_on=[UserCategoryDataset, UserViewsDataset])
    @inputs(Request.user_id, Request.category)
    @outputs(category_view_ratio, num_category_views)
    def extractor_category_view(
        cls,
        ts: pd.Series,
        user_ids: pd.Series,
        categories: pd.Series,
    ):
        category_views, _ = UserCategoryDataset.lookup(  # type: ignore
            ts, user_id=user_ids, category=categories
        )
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        category_views = category_views.fillna(0)
        views = views.fillna(0.001)
        category_views["num_views"] = category_views["num_views"].astype(int)
        category_view_ratio = category_views["num_views"] / views["num_views"]
        return pd.DataFrame(
            {
                "category_view_ratio": category_view_ratio,
                "num_category_views": category_views["num_views"],
            }
        )


@pytest.mark.slow
@mock
def test_social_network(client):
    client.commit(
        message="social network",
        datasets=[
            UserInfo,
            PostInfo,
            ViewData,
            CityInfo,
            UserViewsDataset,
            UserCategoryDataset,
        ],
        featuresets=[Request, UserFeatures],
    )
    user_data_df = pd.read_csv("fennel/client_tests/data/user_data.csv")
    post_data_df = pd.read_csv("fennel/client_tests/data/post_data.csv")
    view_data_df = pd.read_csv("fennel/client_tests/data/view_data_sampled.csv")
    ts = "2018-01-01 00:00:00"
    user_data_df["timestamp"] = ts
    post_data_df["timestamp"] = ts
    view_data_df["time_stamp"] = view_data_df["time_stamp"].apply(
        lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M %p")
    )
    res = client.log("fennel_webhook", "UserInfo", user_data_df)
    assert res.status_code == requests.codes.OK, res.json()

    res = client.log("fennel_webhook", "PostInfo", post_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    res = client.log("fennel_webhook", "ViewData", view_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    now = datetime.now()
    ts = pd.Series([now, now, now])
    if client.is_integration_client():
        client.sleep(120)
    df, found = CityInfo.lookup(
        ts=ts,
        city=pd.Series(["Wufeng", "Coyaima", "San Angelo"]),
        gender=pd.Series(["Male", "Male", "Female"]),
    )
    assert found.to_list() == [True, True, True]

    feature_df = client.query(
        outputs=[UserFeatures],
        inputs=[Request.user_id, Request.category],
        input_dataframe=pd.DataFrame(
            {
                "Request.user_id": [
                    "5eece14efc13ae6609000000",
                    "5eece14efc13ae660900003c",
                ],
                "Request.category": ["banking", "programming"],
            }
        ),
    )
    assert (
        feature_df["UserFeatures.num_views"].to_list(),
        feature_df["UserFeatures.num_category_views"].to_list(),
        feature_df["UserFeatures.category_view_ratio"].to_list(),
    ) == ([2, 4], [0, 1], [0.0, 0.25])

    if client.is_integration_client():
        return

    df = client.get_dataset_df("UserCategoryDataset")
    assert df.shape == (1998, 4)
