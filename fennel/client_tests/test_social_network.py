from datetime import datetime

import pandas as pd
import requests

from fennel.datasets import dataset, field, Dataset, pipeline
from fennel.featuresets import depends_on
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.aggregate import Count
from fennel.lib.metadata import meta
from fennel.lib.schema import Series
from fennel.lib.schema import regex, oneof
from fennel.lib.window import Window
from fennel.test_lib import mock_client


@meta(owner="data-eng@myspace.com")
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


@dataset
@meta(owner="data-eng@myspace.com")
class PostInfo:
    title: str
    category: str
    post_id: int = field(key=True)
    timestamp: datetime


@meta(owner="data-eng@myspace.com")
@dataset
class ViewData:
    user_id: str
    post_id: int
    time_stamp: datetime


@meta(owner="ml-eng@myspace.com")
@dataset
class CityInfo:
    city: str = field(key=True)
    gender: str = field(key=True)
    count: int
    timestamp: datetime

    @classmethod
    @pipeline(1)
    def count_city_gender(cls, user_info: Dataset[UserInfo]):
        return user_info.groupby(["city", "gender"]).aggregate(
            [Count(window=Window("1y 5s"), into_field="count")]
        )


@dataset
@meta(owner="ml-eng@myspace.com")
class UserViewsDataset:
    user_id: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @classmethod
    @pipeline(1)
    def count_user_views(cls, view_data: Dataset[ViewData]):
        return view_data.groupby("user_id").aggregate(
            [Count(window=Window("3y 8s"), into_field="num_views")]
        )


@dataset
@meta(owner="ml-eng@myspace.com")
class UserCategoryDataset:
    user_id: str = field(key=True)
    category: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @classmethod
    @pipeline(1)
    def count_user_views(
            cls, view_data: Dataset[ViewData], post_info: Dataset[PostInfo]
    ):
        post_info_enriched = view_data.left_join(post_info, on=["post_id"])
        post_info_enriched_t = post_info_enriched.transform(
            lambda df: df.fillna("unknown"),
            schema={"user_id": str, "category": str, "time_stamp": datetime},
        )
        return post_info_enriched_t.groupby("user_id", "category").aggregate(
            [Count(window=Window("3y 8s"), into_field="num_views")]
        )


# --- Featuresets ---


@meta(owner="feature-team@myspace.com")
@featureset
class Request:
    user_id: str = feature(id=1)
    post_id: int = feature(id=2)


@meta(owner="feature-team@myspace.com")
@featureset
class UserFeatures:
    category: str = feature(id=1)  # Input feature
    num_views: int = feature(id=2)
    num_category_views: int = feature(id=3)
    category_view_ratio: float = feature(id=4)

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        views = views.fillna(0)
        return views["num_views"]

    @depends_on(UserCategoryDataset, UserViewsDataset)
    @extractor
    def extractor_category_view(
            cls,
            ts: Series[datetime],
            user_ids: Series[Request.user_id],
            categories: Series[category],
    ) -> Series[category_view_ratio, num_category_views]:
        category_views, _ = UserCategoryDataset.lookup(  # type: ignore
            ts, user_id=user_ids, category=categories
        )
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        category_views = category_views.fillna(0)
        views = views.fillna(0.001)
        category_view_ratio = category_views["num_views"] / views["num_views"]
        return pd.DataFrame(
            {
                "category_view_ratio": category_view_ratio,
                "num_category_views": category_views["num_views"],
            }
        )


@mock_client
def test_social_network(client):
    client.sync(
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
    ts = datetime(2018, 1, 1, 0, 0, 0)
    user_data_df["timestamp"] = ts
    post_data_df["timestamp"] = ts
    view_data_df["time_stamp"] = view_data_df["time_stamp"].apply(
        lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M %p")
    )

    res = client.log("UserInfo", user_data_df)
    assert res.status_code == requests.codes.OK, res.json()

    res = client.log("PostInfo", post_data_df)
    assert res.status_code == requests.codes.OK, res.json()

    res = client.log("ViewData", view_data_df)
    assert res.status_code == requests.codes.OK, res.json()
    now = datetime.now()
    ts = pd.Series([now, now, now])
    df, found = CityInfo.lookup(
        ts=ts,
        city=pd.Series(["Wufeng", "Coyaima", "San Angelo"]),
        gender=pd.Series(["Male", "Male", "Female"]),
    )
    assert found.to_list() == [True, True, True]

    client.extract_features(
        output_feature_list=[UserFeatures],
        input_feature_list=[Request, UserFeatures.category],
        input_dataframe=pd.DataFrame(
            {
                "Request.user_id": ["1", "2", "3"],
                "Request.post_id": [1, 2, 3],
                "UserFeatures.category": ["a", "b", "c"],
            }
        ),
    )
