import hashlib
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.aggregate import Count
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
from fennel.lib.window import Window


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


@dataset
@meta(owner="ml-eng@myspace.com")
class UserViewsDataset:
    user_id: str = field(key=True)
    num_views: int
    time_stamp: datetime

    @classmethod
    @pipeline(id=1)
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
    def count_user_views(cls, view_data: Dataset[ViewData],
                         post_info: Dataset[PostInfo]):
        post_info_enriched = view_data.left_join(post_info, on=["post_id"])
        post_info_enriched_t = post_info_enriched.transform(
            lambda df: df.fillna('unknown'),
            schema={'user_id': str, 'category': str, 'time_stamp': datetime})
        return post_info_enriched_t.groupby("user_id", "category").aggregate(
            [Count(window=Window("3y 8s"), into_field="num_views")]
        )


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

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> Series[num_views]:
        views, _ = UserViewsDataset.lookup(ts, user_id=user_ids)  # type: ignore
        views = views.fillna(0)
        views["num_views"] = views["num_views"].astype(int)
        return views["num_views"]

    @depends_on(UserCategoryDataset, UserViewsDataset)
    @extractor
    def extractor_category_view(
            cls,
            ts: Series[datetime],
            user_ids: Series[Request.user_id],
            categories: Series[Request.category],
    ) -> Series[category_view_ratio, num_category_views]:
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


@meta(owner="feature-team@myspace.com")
@featureset
class UserRandomFeatures:
    user_id_x2: int = feature(id=2)
    user_id_x3: int = feature(id=3)

    @depends_on(UserViewsDataset)
    @extractor
    def extract_user_views(
            cls, ts: Series[datetime], user_ids: Series[Request.user_id]
    ) -> DataFrame[user_id_x2, user_id_x3]:
        df = pd.DataFrame()
        nums = []
        df["user_id"] = user_ids.apply(
            lambda x: int(hashlib.sha256(x.encode('utf-8')).hexdigest(),
                16) % 10 ** 8)
        df["user_id_x2"] = df["user_id"] * 2
        df["user_id_x3"] = df["user_id"] * 3
        return df[["user_id_x2", "user_id_x3"]]
