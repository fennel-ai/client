import csv
import random
import time
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.aggregate import Count
from fennel.lib.metadata import meta
from fennel.lib.schema import Series
from fennel.lib.window import Window
from fennel.test_lib.local_client import LocalClient
from locust import User, events

MS_IN_SECOND = 1000
AGGREGATE = "aggregate"
QUERY = "query"
CATEGORIES = ['graphic', 'Craft', 'politics', 'political', 'Mathematics',
              'zoology', 'business', 'dance', 'banking', 'HR management', 'art',
              'science', 'Music', 'operating system', 'Fashion Design',
              'programming', 'painting', 'photography', 'drawing', 'GST']


class CSVReader:
    "Read test data from csv file using an iterator"

    def __init__(self, file):
        try:
            file = open(file)
        except TypeError:
            pass
        self.file = file
        self.reader = csv.reader(file)

    def __next__(self):
        try:
            return next(self.reader)
        except StopIteration:
            # reuse file on EOF
            self.file.seek(0, 0)
            return next(self.reader)


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


class FennelLocusClient(object):
    def __init__(self, host):
        self.client = LocalClient("http://localhost:50051",
            "http://localhost:3000", None)

    def _get_result_len(self, result):
        if isinstance(result, pd.DataFrame):
            return len(result)
        elif isinstance(result, pd.Series):
            return len(result)
        else:
            return 0

    def _get_elapsed_time(self, start_time):
        return int((time.time() - start_time) * MS_IN_SECOND)

    def get_features(self, user_id_1: str, user_id_2: str, specifier):
        """
        All stats are grouped by request_type & name. The specifier provides a name for the query.
        """
        start_time = time.time()
        failure = True
        err = ""
        try:
            # Pick a random category
            catogory_1 = random.choice(CATEGORIES)
            catogory_2 = random.choice(CATEGORIES)
            result = self.client.client.extract_features(
                output_feature_list=[UserFeatures],
                input_feature_list=[Request],
                input_dataframe=pd.DataFrame(
                    {"Request.user_id": [user_id_1, user_id_2],
                     "Request.category": [catogory_1, catogory_2]}),
            )
            if result.shape == (2, 3):
                failure = False
            else:
                err = "Result shape is not 2x3, it is {}".format(result.shape)
        except Exception as e:
            err = str(e)
            failure = True

        if failure:
            total_time = self._get_elapsed_time(start_time)
            events.request_failure.fire(request_type=QUERY, name=specifier,
                response_time=total_time, exception=err, response_length=0)
            return -1

        total_time = int((time.time() - start_time) * MS_IN_SECOND)
        events.request_success.fire(request_type=QUERY, name=specifier,
            response_time=total_time,
            response_length=self._get_result_len(result))
        return result


class FennelLocustUser(User):
    abstract = True

    def __init__(self, *args, **kwargs):
        super(FennelLocustUser, self).__init__(*args, **kwargs)
        self.client = FennelLocusClient(self.host)
