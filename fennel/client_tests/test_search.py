import unittest
from collections import defaultdict
from datetime import datetime
from typing import List, Dict

import numpy as np
import pandas as pd
import requests

from fennel import sources
from fennel.datasets import dataset, Dataset, pipeline, field
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.aggregate import Count, Sum
from fennel.lib.metadata import meta
from fennel.lib.schema import Embedding
from fennel.lib.schema import Series
from fennel.lib.window import Window
from fennel.sources import source
from fennel.test_lib import mock_client

biq_query = sources.BigQuery(
    name="bg_source",
    project_id="my-project-356105",
    dataset_id="query_data",
    credentials_json="XYZ",
)

s3 = sources.S3(
    name="s3_source",
    bucket="engagement",
    path_prefix="prod/apac/",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
)


################################################################################
#                           Datasets
################################################################################


@meta(owner="e1@company.com")
@source(s3.bucket("engagement", prefix="notion", src_schema={}), every="2m")
@dataset
class NotionDocs:
    doc_id: int = field(key=True)
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@meta(owner="e2@company.com")
@source(s3.bucket("engagement", prefix="coda", src_schema={}))
@dataset
class CodaDocs:
    doc_id: int = field(key=True).meta(owner="aditya@fennel.ai")  # type: ignore
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@meta(owner="e3@company.com")
@source(s3.bucket("engagement", prefix="google", src_schema={}))
@dataset
class GoogleDocs:
    doc_id: int = field(key=True)
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


def doc_pipeline_helper(df: pd.DataFrame, origin: str) -> pd.DataFrame:
    df["origin"] = origin
    return df


@meta(owner="e2@company.com")
@dataset
class Document:
    doc_id: int = field(key=True).meta(owner="aditya@fennel.ai")  # type: ignore
    body: str
    title: str
    owner: str
    origin: str
    creation_timestamp: datetime

    @staticmethod
    @pipeline(NotionDocs)
    def notion_pipe(ds: Dataset):
        return ds.transform(
            lambda df: doc_pipeline_helper(df, "Notion"),
            schema={
                "doc_id": int,
                "body": str,
                "title": str,
                "owner": str,
                "creation_timestamp": datetime,
                "origin": str,
            },
        )

    @staticmethod
    @pipeline(CodaDocs)
    def coda_pipe(ds: Dataset):
        return ds.transform(
            lambda df: doc_pipeline_helper(df, "Coda"),
            schema={
                "doc_id": int,
                "body": str,
                "title": str,
                "owner": str,
                "creation_timestamp": datetime,
                "origin": str,
            },
        )

    @staticmethod
    @pipeline(GoogleDocs)
    def google_docs_pipe(ds: Dataset):
        return ds.transform(
            lambda df: doc_pipeline_helper(df, "GoogleDocs"),
            schema={
                "doc_id": int,
                "body": str,
                "title": str,
                "owner": str,
                "creation_timestamp": datetime,
                "origin": str,
            },
        )


class HuggingFace:
    @staticmethod
    def get_bert_embedding(text: str):
        return np.random.rand(128)

    @staticmethod
    def get_fasttext_embedding(text: str):
        return np.random.rand(256)


def get_top_10_unique_words(text: str):
    words = text.split(" ")
    counter: Dict[str, int] = defaultdict(int)
    for w in words:
        counter[w] += 1
    res = sorted(counter.items(), key=lambda kv: kv[1])
    return [x[0] for x in res]


def get_content_features(df: pd.DataFrame) -> pd.DataFrame:
    df["bert_embedding"] = df["body"].apply(
        lambda x: HuggingFace.get_bert_embedding(x)
    )
    df["fast_text_embedding"] = df["body"].apply(
        lambda x: HuggingFace.get_fasttext_embedding(x)
    )
    df["num_words"] = df["body"].apply(lambda x: len(x.split(" ")))
    df["num_stop_words"] = df["body"].apply(
        lambda x: len([x for x in x.split(" ") if x in ["the", "is", "of"]])
    )
    df["top_10_unique_words"] = df["body"].apply(
        lambda x: get_top_10_unique_words(x)
    )
    return df[
        [
            "doc_id",
            "bert_embedding",
            "fast_text_embedding",
            "num_words",
            "num_stop_words",
            "top_10_unique_words",
            "creation_timestamp",
        ]
    ]


@meta(owner="sagar@oslash.ai")
@dataset
class DocumentContentDataset:
    doc_id: int = field(key=True)
    bert_embedding: Embedding[128]
    fast_text_embedding: Embedding[256]
    num_words: int
    num_stop_words: int
    top_10_unique_words: List[str]
    creation_timestamp: datetime

    @staticmethod
    @pipeline(Document)
    def content_features(
        ds: Dataset,
    ):
        return ds.transform(
            get_content_features,
            schema={
                "doc_id": int,
                "bert_embedding": Embedding[128],
                "fast_text_embedding": Embedding[256],
                "num_words": int,
                "num_stop_words": int,
                "top_10_unique_words": List[str],
                "creation_timestamp": datetime,
            },
        )


@meta(owner="aditya@fennel.ai")
@source(biq_query.table("user_activity", cursor_field="timestamp"), every="1h")
@dataset
class UserActivity:
    user_id: int
    doc_id: int
    action_type: str
    view_time: float
    timestamp: datetime


@meta(owner="sagar@fennel.ai")
@dataset
class UserEngagementDataset:
    user_id: int = field(key=True)
    num_views: int
    num_short_views_7d: int
    num_long_views: int
    timestamp: datetime

    @staticmethod
    @pipeline(UserActivity)
    def user_engagement_pipeline(ds: Dataset):
        def create_short_click(df: pd.DataFrame) -> pd.DataFrame:
            df["is_short_click"] = df["view_time"] < 5
            df["is_long_click"] = df["view_time"] >= 5
            return df

        click_type = ds.transform(
            create_short_click,
            schema={
                "user_id": int,
                "doc_id": int,
                "action_type": str,
                "view_time": float,
                "timestamp": datetime,
                "is_short_click": bool,
                "is_long_click": bool,
            },
        )
        return click_type.groupby("user_id").aggregate(
            [
                Count(window=Window("forever"), into_field="num_views"),
                Sum(
                    window=Window("7d"),
                    of="is_short_click",
                    into_field="num_short_views_7d",
                ),
                Sum(
                    window=Window("forever"),
                    of="is_long_click",
                    into_field="num_long_views",
                ),
            ]
        )


@meta(owner="aditya@fennel.ai")
@dataset
class DocumentEngagementDataset:
    doc_id: int = field(key=True)
    num_views: int
    num_views_7d: int
    num_views_28d: int
    total_timespent: float
    timestamp: datetime

    @staticmethod
    @pipeline(UserActivity)
    def doc_engagement_pipeline(ds: Dataset):
        return ds.groupby("doc_id").aggregate(
            [
                Count(window=Window("forever"), into_field="num_views"),
                Count(window=Window("7d"), into_field="num_views_7d"),
                Count(window=Window("28d"), into_field="num_views_28d"),
                Sum(
                    window=Window("forever"),
                    of="view_time",
                    into_field="total_timespent",
                ),
            ]
        )


################################################################################
#                           Featuresets
################################################################################


@featureset
class Query:
    doc_id: int = feature(id=1)
    user_id: int = feature(id=2)


@featureset
class UserBehaviorFeatures:
    user_id: int = feature(id=1)
    num_views: int = feature(id=2).meta(  # type: ignore
        owner="aditya@fennel.ai"
    )
    num_short_views_7d: int = feature(id=3)
    num_long_views: int = feature(id=4)

    @extractor
    @depends_on(UserEngagementDataset)
    def get_features(ts: Series[datetime], user_id: Series[Query.user_id]):
        df, found = UserEngagementDataset.lookup(  # type: ignore
            ts, user_id=user_id  # type: ignore
        )
        df.drop("timestamp", axis=1, inplace=True)
        return df


@featureset
class DocumentFeatures:
    doc_id: int = feature(id=1)
    num_views: int = feature(id=2)
    num_views_7d: int = feature(id=3)
    total_timespent_minutes: float = feature(id=4)
    num_views_28d: int = feature(id=5)

    @extractor
    @depends_on(DocumentEngagementDataset)
    def get_features(ts: Series[datetime], doc_id: Series[Query.doc_id]):
        df, found = DocumentEngagementDataset.lookup(  # type: ignore
            ts, user_id=doc_id  # type: ignore
        )
        df["total_timespent_minutes"] = df["total_timespent"] / 60
        df.drop("total_timespent", axis=1, inplace=True)
        df.drop("timestamp", axis=1, inplace=True)
        return df


@featureset
class DocumentContentFeatures:
    doc_id: int = feature(id=1)
    bert_embedding: Embedding[128] = feature(id=2)
    fast_text_embedding: Embedding[256] = feature(id=3)
    num_words: int = feature(id=4)
    num_stop_words: int = feature(id=5)
    top_10_unique_words: List[str] = feature(id=6)

    @extractor
    @depends_on(DocumentContentDataset)
    def get_features(ts: Series[datetime], doc_id: Series[Query.doc_id]):
        df, found = DocumentContentDataset.lookup(  # type: ignore
            ts, user_id=doc_id  # type: ignore
        )
        df.drop("creation_timestamp", axis=1, inplace=True)
        return df


class TestSearchExample(unittest.TestCase):
    def log_document_data(self, client):
        now = datetime.now()
        data = [
            [141234, "This is a random document", "Random Title", "Sagar", now],
            [
                143234,
                "This is a rand document with words",
                "Some Words",
                "Shoaib",
                now,
            ],
            [
                143354,
                "This is the third document",
                "Some more words",
                "Ankit",
                now,
            ],
            [
                143244,
                "This is the last and final document",
                "Final Title",
                "Sagar",
                now,
            ],
        ]
        columns = ["doc_id", "body", "title", "owner", "creation_timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("NotionDocs", df)
        assert response.status_code == requests.codes.OK, response.json()

        yesterday = now - pd.Timedelta(days=1)
        data = [
            [
                31234,
                "This is a random Coda document",
                "[Coda]Random Title",
                "Sagar",
                yesterday,
            ],
            [
                33234,
                "This is a rand document in Coda with words",
                "[Coda]Some Words",
                "Shoaib",
                yesterday,
            ],
            [
                33354,
                "This is the third documentin Coda",
                "[Coda]Some more words",
                "Ankit",
                yesterday,
            ],
            [
                33244,
                "This is the last and final document in Coda",
                "[Coda]Final Title",
                "Sagar",
                yesterday,
            ],
        ]
        columns = ["doc_id", "body", "title", "owner", "creation_timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("CodaDocs", df)
        assert response.status_code == requests.codes.OK, response.json()

    def log_engagement_data(self, client):
        now = datetime.now()
        data = [
            [123, 31234, "view", 5, now],
            [123, 143354, "view", 1, now],
            [123, 33234, "view", 2, now],
            [342, 141234, "view", 5, now],
            [342, 33234, "view", 7, now],
            [342, 141234, "view", 9, now],
        ]
        columns = ["user_id", "doc_id", "action_type", "view_time", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

    @mock_client
    def test_datasets(self, client):
        client.sync(datasets=[NotionDocs, CodaDocs, GoogleDocs, Document])
        self.log_document_data(client)
        now = datetime.now()
        yesterday = now - pd.Timedelta(days=1)

        doc_ids = pd.Series([141234, 143354, 33234, 11111])
        ts = pd.Series([now, now, now, now])
        df, found = Document.lookup(ts, doc_id=doc_ids)
        assert df.shape == (4, 6)
        assert found.tolist() == [True, True, True, False]

        doc_ids = pd.Series([141234, 143354, 33234, 11111])
        ts = pd.Series([yesterday, yesterday, yesterday, yesterday])
        df, found = Document.lookup(ts, doc_id=doc_ids)
        assert df.shape == (4, 6)
        assert found.tolist() == [False, False, True, False]

        client._reset()
        client.sync(
            datasets=[
                UserActivity,
                UserEngagementDataset,
                DocumentEngagementDataset,
            ]
        )

        self.log_engagement_data(client)
        now = datetime.now()
        ts = pd.Series([now, now])
        user_ids = pd.Series([123, 342])
        df, found = UserEngagementDataset.lookup(ts, user_id=user_ids)
        assert df.shape == (2, 5)
        assert found.tolist() == [True, True]

        ts = pd.Series([now, now, now])
        doc_ids = pd.Series([31234, 143354, 33234])
        df, found = DocumentEngagementDataset.lookup(ts, doc_id=doc_ids)
        assert df.shape == (3, 6)
        assert found.tolist() == [True, True, True]

        # Reset the client
        client._reset()
        client.sync(
            datasets=[
                NotionDocs,
                CodaDocs,
                GoogleDocs,
                Document,
                UserActivity,
                DocumentContentDataset,
                UserEngagementDataset,
                DocumentEngagementDataset,
            ],
            featuresets=[
                Query,
                UserBehaviorFeatures,
                DocumentFeatures,
                DocumentContentFeatures,
            ],
        )

        self.log_document_data(client)
        self.log_engagement_data(client)
        input_df = pd.DataFrame(
            {
                "Query.user_id": [123, 342],
                "Query.doc_id": [31234, 33234],
            }
        )
        df = client.extract_features(
            output_feature_list=[
                UserBehaviorFeatures,
                DocumentFeatures,
                DocumentContentFeatures,
            ],
            input_feature_list=[Query],
            input_df=input_df,
        )
        assert df.shape == (2, 15)
