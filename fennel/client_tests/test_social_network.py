from datetime import datetime
from typing import List

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import LastK
from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field, Dataset, pipeline, Count
from fennel.dtypes import regex, oneof, Continuous
from fennel.featuresets import featureset, feature as F, extractor
from fennel.lib import meta, inputs, outputs
from fennel.testing import mock, log
from fennel.expr import col

webhook = Webhook(name="fennel_webhook")


@meta(owner="data-eng@myspace.com")
@source(webhook.endpoint("UserInfo"), cdc="upsert", disorder="14d")
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


@source(webhook.endpoint("PostInfo"), disorder="14d", cdc="upsert")
@dataset(index=True)
@meta(owner="data-eng@myspace.com")
class PostInfo:
    title: str
    category: str  # type: ignore
    post_id: int = field(key=True)
    timestamp: datetime


@source(
    webhook.endpoint("PostInfoWithRightFields"), disorder="14d", cdc="upsert"
)
@dataset(index=True)
@meta(owner="data-eng@myspace.com")
class PostInfoWithRightFields:
    title: str
    category: str  # type: ignore
    post_id: int = field(key=True)
    timestamp: datetime
    extra_field: str


@meta(owner="data-eng@myspace.com")
@dataset
@source(webhook.endpoint("ViewData"), disorder="14d", cdc="append")
class ViewData:
    user_id: str
    post_id: int
    time_stamp: datetime


@meta(owner="ml-eng@myspace.com")
@dataset(index=True)
class CityInfo:
    city: str = field(key=True)
    gender: oneof(str, ["Female", "Male"]) = field(key=True)  # type: ignore
    count: int
    timestamp: datetime

    @pipeline
    @inputs(UserInfo)
    def count_city_gender(cls, user_info: Dataset):
        return user_info.groupby(["city", "gender"]).aggregate(
            [Count(window=Continuous("6y 8s"), into_field="count")]
        )


@meta(owner="ml-eng@myspace.com")
@dataset(index=True)
class UserViewsDataset:
    user_id: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @pipeline
    @inputs(ViewData)
    def count_user_views(cls, view_data: Dataset):
        return view_data.groupby("user_id").aggregate(
            [Count(window=Continuous("6y 8s"), into_field="num_views")]
        )


@meta(owner="ml-eng@myspace.com")
@dataset(index=True)
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
            [Count(window=Continuous("6y 8s"), into_field="num_views")]
        )


@meta(owner="ml-eng@myspace.com")
@dataset(index=True)
class UserCategoryDatasetWithRightFields:
    user_id: str = field(key=True)
    category: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @pipeline
    @inputs(ViewData, PostInfoWithRightFields)
    def count_user_views(cls, view_data: Dataset, post_info: Dataset):
        post_info_enriched = view_data.join(
            post_info, how="inner", on=["post_id"], fields=["title", "category"]
        )
        return post_info_enriched.groupby("user_id", "category").aggregate(
            [Count(window=Continuous("6y 8s"), into_field="num_views")]
        )


@meta(owner="ml-eng@myspace.com")
@dataset(index=True)
class LastViewedPost:
    user_id: str = field(key=True)
    post_id: int
    time_stamp: datetime

    @pipeline
    @inputs(ViewData)
    def last_viewed_post(cls, view_data: Dataset):
        return view_data.groupby("user_id").latest()


@meta(owner="ml-eng@myspace.com")
@dataset(index=True)
class LastViewedPostByAgg:
    user_id: str = field(key=True)
    post_id: List[int]
    time_stamp: datetime

    @pipeline
    @inputs(ViewData)
    def last_viewed_post(cls, view_data: Dataset):
        return view_data.groupby("user_id").aggregate(
            LastK(
                into_field="post_id",
                of="post_id",
                window=Continuous("forever"),
                limit=1,
                dedup=False,
            )
        )


# --- Featuresets ---


@meta(owner="feature-team@myspace.com")
@featureset
class Request:
    user_id: str
    category: str


@meta(owner="feature-team@myspace.com")
@featureset
class UserFeatures:
    user_id: str = F(Request.user_id)  # type: ignore
    num_views: int
    category: str = F(Request.category)  # type: ignore
    num_category_views: int = F(UserCategoryDataset.num_views, default=0)  # type: ignore
    category_view_ratio: float = F(col("num_category_views") / col("num_views"))
    last_viewed_post: int = F(LastViewedPost.post_id, default=-1)  # type: ignore
    last_viewed_post2: List[int] = F(
        LastViewedPostByAgg.post_id, default=[-1]  # type: ignore
    )

    @extractor(deps=[UserViewsDataset])  # type: ignore
    @inputs(Request.user_id)
    @outputs("num_views")
    def extract_user_views(cls, ts: pd.Series, user_ids: pd.Series):
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        views = views.fillna(0)
        return views["num_views"]


@meta(owner="feature-team@myspace.com")
@featureset
class UserFeaturesWithRightFields:
    user_id: str = F(Request.user_id)  # type: ignore
    num_views: int
    category: str = F(Request.category)  # type: ignore
    num_category_views: int = F(UserCategoryDatasetWithRightFields.num_views, default=0)  # type: ignore
    category_view_ratio: float = F(col("num_category_views") / col("num_views"))
    last_viewed_post: int = F(LastViewedPost.post_id, default=-1)  # type: ignore
    last_viewed_post2: List[int] = F(
        LastViewedPostByAgg.post_id, default=[-1]  # type: ignore
    )

    @extractor(deps=[UserViewsDataset])  # type: ignore
    @inputs(Request.user_id)
    @outputs("num_views")
    def extract_user_views(cls, ts: pd.Series, user_ids: pd.Series):
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        views = views.fillna(0)
        return views["num_views"]


@pytest.mark.integration
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
            LastViewedPost,
            LastViewedPostByAgg,
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
    # # Advance all timestamps by 6 years
    user_data_df["timestamp"] = pd.to_datetime(
        user_data_df["timestamp"]
    ) + pd.DateOffset(years=4)
    post_data_df["timestamp"] = pd.to_datetime(
        post_data_df["timestamp"]
    ) + pd.DateOffset(years=4)
    view_data_df["time_stamp"] = view_data_df["time_stamp"] + pd.DateOffset(
        years=4
    )

    res = client.log("fennel_webhook", "UserInfo", user_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    res = client.log("fennel_webhook", "PostInfo", post_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    res = client.log("fennel_webhook", "ViewData", view_data_df)
    assert res.status_code == requests.codes.OK, res.json()

    if client.is_integration_client():
        client.sleep(120)

    keys = pd.DataFrame(
        {
            "city": ["Wufeng", "Coyaima", "San Angelo"],
            "gender": ["Male", "Male", "Female"],
        }
    )

    df, found = client.lookup(
        "CityInfo",
        keys=keys,
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

    # Assert that both the last_viewed_post and last_viewed_post2 features are extracted correctly
    last_post_viewed = feature_df["UserFeatures.last_viewed_post"].to_list()
    last_post_viewed2 = [
        x[0] for x in feature_df["UserFeatures.last_viewed_post2"].to_list()
    ]
    assert last_post_viewed == [936609766, 735291550]
    assert last_post_viewed2 == last_post_viewed

    if client.is_integration_client():
        return
    df = client.get_dataset_df("UserCategoryDataset")
    assert df.shape == (1998, 4)


@pytest.mark.integration
@mock
def test_social_network_with_fields(client):
    client.commit(
        message="social network",
        datasets=[
            UserInfo,
            PostInfoWithRightFields,
            ViewData,
            CityInfo,
            UserViewsDataset,
            UserCategoryDatasetWithRightFields,
            LastViewedPost,
            LastViewedPostByAgg,
        ],
        featuresets=[Request, UserFeaturesWithRightFields],
    )
    user_data_df = pd.read_csv("fennel/client_tests/data/user_data.csv")
    post_data_df = pd.read_csv("fennel/client_tests/data/post_data.csv")
    post_data_len = len(post_data_df.index)
    post_data_df["extra_field"] = list(range(0, post_data_len))
    view_data_df = pd.read_csv("fennel/client_tests/data/view_data_sampled.csv")
    ts = "2018-01-01 00:00:00"
    user_data_df["timestamp"] = ts
    post_data_df["timestamp"] = ts
    view_data_df["time_stamp"] = view_data_df["time_stamp"].apply(
        lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M %p")
    )
    # # Advance all timestamps by 6 years
    user_data_df["timestamp"] = pd.to_datetime(
        user_data_df["timestamp"]
    ) + pd.DateOffset(years=4)
    post_data_df["timestamp"] = pd.to_datetime(
        post_data_df["timestamp"]
    ) + pd.DateOffset(years=4)
    view_data_df["time_stamp"] = view_data_df["time_stamp"] + pd.DateOffset(
        years=4
    )

    res = client.log("fennel_webhook", "UserInfo", user_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    res = client.log("fennel_webhook", "PostInfoWithRightFields", post_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    res = client.log("fennel_webhook", "ViewData", view_data_df)
    assert res.status_code == requests.codes.OK, res.json()

    if client.is_integration_client():
        client.sleep(120)

    keys = pd.DataFrame(
        {
            "city": ["Wufeng", "Coyaima", "San Angelo"],
            "gender": ["Male", "Male", "Female"],
        }
    )

    df, found = client.lookup(
        "CityInfo",
        keys=keys,
    )
    assert found.to_list() == [True, True, True]

    feature_df = client.query(
        outputs=[UserFeaturesWithRightFields],
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
        feature_df["UserFeaturesWithRightFields.num_views"].to_list(),
        feature_df["UserFeaturesWithRightFields.num_category_views"].to_list(),
        feature_df["UserFeaturesWithRightFields.category_view_ratio"].to_list(),
    ) == ([2, 4], [0, 1], [0.0, 0.25])

    # Assert that both the last_viewed_post and last_viewed_post2 features are extracted correctly
    last_post_viewed = feature_df[
        "UserFeaturesWithRightFields.last_viewed_post"
    ].to_list()
    last_post_viewed2 = [
        x[0]
        for x in feature_df[
            "UserFeaturesWithRightFields.last_viewed_post2"
        ].to_list()
    ]
    assert last_post_viewed == [936609766, 735291550]
    assert last_post_viewed2 == last_post_viewed

    if client.is_integration_client():
        return
    df = client.get_dataset_df("UserCategoryDatasetWithRightFields")
    assert "extra_field" not in df.columns
    assert df.shape == (1998, 4)


@mock
def test_social_network_with_mock_log(client):
    client.commit(
        message="social network",
        datasets=[
            UserInfo,
            PostInfo,
            ViewData,
            CityInfo,
            UserViewsDataset,
            UserCategoryDataset,
            LastViewedPost,
            LastViewedPostByAgg,
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
    # # Advance all timestamps by 6 years
    user_data_df["timestamp"] = pd.to_datetime(
        user_data_df["timestamp"]
    ) + pd.DateOffset(years=4)
    post_data_df["timestamp"] = pd.to_datetime(
        post_data_df["timestamp"]
    ) + pd.DateOffset(years=4)
    view_data_df["time_stamp"] = view_data_df["time_stamp"] + pd.DateOffset(
        years=4
    )

    log(UserInfo, user_data_df)
    log(PostInfo, post_data_df)
    log(ViewData, view_data_df)

    keys = pd.DataFrame(
        {
            "city": ["Wufeng", "Coyaima", "San Angelo"],
            "gender": ["Male", "Male", "Female"],
        }
    )

    df, found = client.lookup(
        "CityInfo",
        keys=keys,
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

    # Assert that both the last_viewed_post and last_viewed_post2 features are extracted correctly
    last_post_viewed = feature_df["UserFeatures.last_viewed_post"].to_list()
    last_post_viewed2 = [
        x[0] for x in feature_df["UserFeatures.last_viewed_post2"].to_list()
    ]
    assert last_post_viewed == [936609766, 735291550]
    assert last_post_viewed2 == last_post_viewed

    df = client.get_dataset_df("UserCategoryDataset")
    assert df.shape == (1998, 4)
