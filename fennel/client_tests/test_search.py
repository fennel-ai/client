import unittest
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from typing import Dict, List

import fennel._vendor.requests as requests
from fennel import sources
from fennel.datasets import dataset, Dataset, pipeline, field
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.aggregate import Count, Sum
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import Embedding
from fennel.lib.schema import inputs, outputs
from fennel.lib.window import Window
from fennel.sources import source
from fennel.test_lib import mock

biq_query = sources.BigQuery(
    name="bg_source",
    project_id="my-project-356105",
    dataset_id="query_data",
    credentials_json="{}",
)

s3 = sources.S3(
    name="s3_source",
    bucket="engagement",
    path_prefix="prod/apac/",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
)

webhook = sources.Webhook(name="fennel_webhook")


################################################################################
#                           Datasets
################################################################################


@meta(owner="e1@company.com")
@source(s3.bucket("engagement", prefix="notion"), every="2m")
@dataset
class NotionDocs:
    doc_id: int = field(key=True)
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@meta(owner="e2@company.com")
@source(s3.bucket("engagement", prefix="coda"))
@dataset
class CodaDocs:
    doc_id: int = field(key=True).meta(owner="aditya@fennel.ai")  # type: ignore
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@meta(owner="e3@company.com")
@source(s3.bucket("engagement", prefix="google"))
@dataset
class GoogleDocs:
    doc_id: int = field(key=True)
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@meta(owner="e2@company.com")
@dataset
class Document:
    doc_id: int = field(key=True).meta(owner="aditya@fennel.ai")  # type: ignore
    body: str
    title: str
    owner: str
    origin: str
    creation_timestamp: datetime

    @pipeline()
    @inputs(NotionDocs, CodaDocs, GoogleDocs)
    def notion_pipe(
        cls, notion_docs: Dataset, coda_docs: Dataset, google_docs: Dataset
    ):
        new_schema = notion_docs.schema()
        new_schema["origin"] = str
        return (
            notion_docs.transform(
                lambda df: cls.doc_pipeline_helper(df, "Notion"),
                schema=new_schema,
            )
            + coda_docs.transform(
                lambda df: cls.doc_pipeline_helper(df, "Coda"),
                schema=new_schema,
            )
            + google_docs.transform(
                lambda df: cls.doc_pipeline_helper(df, "GoogleDocs"),
                schema=new_schema,
            )
        )

    @classmethod
    def doc_pipeline_helper(cls, df: pd.DataFrame, origin: str) -> pd.DataFrame:
        df["origin"] = origin
        return df


def get_bert_embedding(text: str):
    return np.random.rand(128)


def get_fasttext_embedding(text: str):
    return np.random.rand(256)


def get_top_10_unique_words(text: str):
    words = text.split(" ")
    counter: Dict[str, int] = defaultdict(int)
    for w in words:
        counter[w] += 1
    res = sorted(counter.items(), key=lambda kv: kv[1])
    return [x[0] for x in res]


@includes(get_bert_embedding, get_fasttext_embedding, get_top_10_unique_words)
def get_content_features(df: pd.DataFrame) -> pd.DataFrame:
    df["bert_embedding"] = df["body"].apply(lambda x: get_bert_embedding(x))
    df["fast_text_embedding"] = df["body"].apply(
        lambda x: get_fasttext_embedding(x)
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


@meta(owner="test@fennel.ai")
@dataset
class DocumentContentDataset:
    doc_id: int = field(key=True)
    bert_embedding: Embedding[128]
    fast_text_embedding: Embedding[256]
    num_words: int
    num_stop_words: int
    top_10_unique_words: List[str]
    creation_timestamp: datetime

    @pipeline(version=1)
    @inputs(Document)
    def content_features(cls, ds: Dataset):
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


@meta(owner="abhay@fennel.ai")
@dataset
class TopWordsCount:
    word: str = field(key=True)
    count: int
    timestamp: datetime

    @pipeline(version=1)
    @inputs(DocumentContentDataset)
    def top_words_count(cls, ds: Dataset):
        ds = ds.explode(columns=["top_10_unique_words"]).rename(
            columns={
                "top_10_unique_words": "word",
                "creation_timestamp": "timestamp",
            }
        )  # type: ignore
        schema = ds.schema()
        schema["word"] = str
        ds = ds.transform(lambda df: df, schema)  # type: ignore
        return ds.groupby(["word"]).aggregate(
            [
                Count(window=Window("forever"), into_field="count"),
            ]
        )  # type: ignore


@meta(owner="aditya@fennel.ai")
@source(biq_query.table("user_activity", cursor="timestamp"), every="1h")
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

    @pipeline(version=1)
    @inputs(UserActivity)
    def user_engagement_pipeline(cls, ds: Dataset):
        def create_short_click(df: pd.DataFrame) -> pd.DataFrame:
            df["is_short_click"] = df["view_time"] < 5
            df["is_long_click"] = df["view_time"] >= 5
            df["is_short_click"].astype(int)
            df["is_long_click"].astype(int)
            return df

        click_type = ds.dedup(
            by=["user_id", "doc_id", "action_type"]
        ).transform(
            create_short_click,
            schema={
                "user_id": int,
                "doc_id": int,
                "action_type": str,
                "view_time": float,
                "timestamp": datetime,
                "is_short_click": int,
                "is_long_click": int,
            },
        )
        return click_type.groupby("user_id").aggregate(
            [
                Count(window=Window("forever"), into_field=str(cls.num_views)),
                Sum(
                    window=Window("7d"),
                    of="is_short_click",
                    into_field=str(cls.num_short_views_7d),
                ),
                Sum(
                    window=Window("forever"),
                    of="is_long_click",
                    into_field=str(cls.num_long_views),
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

    @pipeline(version=1)
    @inputs(UserActivity)
    def doc_engagement_pipeline(cls, ds: Dataset):
        return (
            ds.dedup(by=["user_id", "doc_id"])
            .groupby("doc_id")
            .aggregate(
                [
                    Count(
                        window=Window("forever"), into_field=str(cls.num_views)
                    ),
                    Count(
                        window=Window("7d"), into_field=str(cls.num_views_7d)
                    ),
                    Count(
                        window=Window("28d"), into_field=str(cls.num_views_28d)
                    ),
                    Sum(
                        window=Window("forever"),
                        of="view_time",
                        into_field=str(cls.total_timespent),
                    ),
                ]
            )
        )


################################################################################
#                           Featuresets
################################################################################


@meta(owner="aditya@fennel.ai")
@featureset
class Query:
    doc_id: int = feature(id=1)
    user_id: int = feature(id=2)


@meta(owner="aditya@fennel.ai")
@featureset
class UserBehaviorFeatures:
    user_id: int = feature(id=1)
    num_views: int = feature(id=2).meta(  # type: ignore
        owner="aditya@fennel.ai"
    )
    num_short_views_7d: int = feature(id=3)
    num_long_views: int = feature(id=4)

    @extractor(depends_on=[UserEngagementDataset])
    @inputs(Query.user_id)
    def get_user_features(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserEngagementDataset.lookup(  # type: ignore
            ts, user_id=user_id  # type: ignore
        )
        return df


@meta(owner="aditya@fennel.ai")
@featureset
class DocumentFeatures:
    doc_id: int = feature(id=1)
    num_views: int = feature(id=2)
    num_views_7d: int = feature(id=3)
    total_timespent_minutes: float = feature(id=4)
    num_views_28d: int = feature(id=5)

    @extractor(depends_on=[DocumentEngagementDataset])
    @inputs(Query.doc_id)
    def get_doc_features(cls, ts: pd.Series, doc_id: pd.Series):
        df, found = DocumentEngagementDataset.lookup(  # type: ignore
            ts, doc_id=doc_id  # type: ignore
        )
        df[str(cls.total_timespent_minutes)] = df["total_timespent"] / 60
        df.drop("total_timespent", axis=1, inplace=True)
        return df


@meta(owner="aditya@fennel.ai")
@featureset
class DocumentContentFeatures:
    doc_id: int = feature(id=1)
    bert_embedding: Embedding[128] = feature(id=2)
    fast_text_embedding: Embedding[256] = feature(id=3)
    num_words: int = feature(id=4)
    num_stop_words: int = feature(id=5)
    top_10_unique_words: List[str] = feature(id=6)

    @extractor(depends_on=[DocumentContentDataset])
    @inputs(Query.doc_id)
    def get_features(cls, ts: pd.Series, doc_id: pd.Series):
        df, found = DocumentContentDataset.lookup(  # type: ignore
            ts, doc_id=doc_id  # type: ignore
        )
        df.drop("creation_timestamp", axis=1, inplace=True)
        return df


@meta(owner="abhay@fennel.ai")
@featureset
class TopWordsFeatures:
    word: str = feature(id=1)
    count: int = feature(id=2)

    @extractor(depends_on=[TopWordsCount])
    @inputs(word)
    @outputs(count)
    def get_counts(cls, ts: pd.Series, word: pd.Series):
        df, _ = TopWordsCount.lookup(ts, word=word)  # type: ignore
        df = df.fillna(0)
        df["count"] = df["count"].astype(int)
        return df["count"]


class TestSearchExample(unittest.TestCase):
    def log_document_data(self, client):
        now = datetime.utcnow()
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
        response = client.log("fennel_webhook", "NotionDocs", df)

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
        response = client.log("fennel_webhook", "CodaDocs", df)

        assert response.status_code == requests.codes.OK, response.json()

    def log_engagement_data(self, client):
        now = datetime.utcnow()
        data = [
            [123, 31234, "view", 5, now],
            [123, 143354, "view", 1, now],
            [123, 33234, "view", 2, now],
            [342, 141234, "view", 5, now],
            [342, 141234, "view", 5, now],  # duplicate entry
            [342, 33234, "view", 7, now],
            [342, 33234, "view", 7, now],  # duplicate entry
            [342, 141234, "view", 9, now],
        ]
        columns = ["user_id", "doc_id", "action_type", "view_time", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

    @pytest.mark.integration
    @mock
    def test_search_datasets1(self, client):
        if client.integration_mode() == "local":
            pytest.skip("Skipping integration test in local mode")

        mock_NotionDocs = NotionDocs.with_source(webhook.endpoint("NotionDocs"))
        mock_CodaDocs = CodaDocs.with_source(webhook.endpoint("CodaDocs"))
        mock_GoogleDocs = GoogleDocs.with_source(webhook.endpoint("GoogleDocs"))

        client.sync(
            datasets=[mock_NotionDocs, mock_CodaDocs, mock_GoogleDocs, Document]
        )
        self.log_document_data(client)
        client.sleep()
        now = datetime.utcnow()
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

    @pytest.mark.integration
    @mock
    def test_search_datasets2(self, client):
        if client.integration_mode() == "local":
            pytest.skip("Skipping integration test in local mode")

        mock_UserActivity = UserActivity.with_source(
            webhook.endpoint("UserActivity")
        )

        client.sync(
            datasets=[
                mock_UserActivity,
                UserEngagementDataset,
                DocumentEngagementDataset,
            ]
        )

        self.log_engagement_data(client)
        client.sleep(20)
        now = datetime.utcnow()
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

    @pytest.mark.integration
    @mock
    def test_search_e2e(self, client):
        if client.integration_mode() == "local":
            pytest.skip("Skipping integration test in local mode")

        mock_NotionDocs = NotionDocs.with_source(webhook.endpoint("NotionDocs"))
        mock_CodaDocs = CodaDocs.with_source(webhook.endpoint("CodaDocs"))
        mock_GoogleDocs = GoogleDocs.with_source(webhook.endpoint("GoogleDocs"))
        mock_UserActivity = UserActivity.with_source(
            webhook.endpoint("UserActivity")
        )

        client.sync(
            datasets=[
                mock_NotionDocs,
                mock_CodaDocs,
                mock_GoogleDocs,
                Document,
                mock_UserActivity,
                DocumentContentDataset,
                TopWordsCount,
                UserEngagementDataset,
                DocumentEngagementDataset,
            ],
            featuresets=[
                Query,
                UserBehaviorFeatures,
                DocumentFeatures,
                DocumentContentFeatures,
                TopWordsFeatures,
            ],
        )

        self.log_document_data(client)
        client.sleep()
        self.log_engagement_data(client)

        client.sleep(60)
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
            input_dataframe=input_df,
        )
        assert df.shape == (2, 15)
        assert df.columns.tolist() == [
            "UserBehaviorFeatures.user_id",
            "UserBehaviorFeatures.num_views",
            "UserBehaviorFeatures.num_short_views_7d",
            "UserBehaviorFeatures.num_long_views",
            "DocumentFeatures.doc_id",
            "DocumentFeatures.num_views",
            "DocumentFeatures.num_views_7d",
            "DocumentFeatures.total_timespent_minutes",
            "DocumentFeatures.num_views_28d",
            "DocumentContentFeatures.doc_id",
            "DocumentContentFeatures.bert_embedding",
            "DocumentContentFeatures.fast_text_embedding",
            "DocumentContentFeatures.num_words",
            "DocumentContentFeatures.num_stop_words",
            "DocumentContentFeatures.top_10_unique_words",
        ]
        assert df["DocumentContentFeatures.doc_id"].tolist() == [31234, 33234]
        assert df["UserBehaviorFeatures.num_short_views_7d"].tolist() == [2, 0]
        assert df["DocumentFeatures.num_views_28d"].tolist() == [1, 2]

        assert df["DocumentContentFeatures.top_10_unique_words"].tolist()[
            0
        ] == ["This", "is", "a", "random", "Coda", "document"]
        assert df["DocumentContentFeatures.top_10_unique_words"].tolist()[
            1
        ] == [
            "This",
            "is",
            "a",
            "rand",
            "document",
            "in",
            "Coda",
            "with",
            "words",
        ]

        if client.is_integration_client():
            client.sleep(120)
        input_df = pd.DataFrame(
            {
                "TopWordsFeatures.word": ["This", "Coda"],
            }
        )
        df = client.extract_features(
            output_feature_list=[TopWordsFeatures.count],
            input_feature_list=[TopWordsFeatures.word],
            input_dataframe=input_df,
        )
        assert df.shape == (2, 1)
        assert df.columns.tolist() == [
            "TopWordsFeatures.count",
        ]
        assert df["TopWordsFeatures.count"].tolist() == [8, 4]

        if client.is_integration_client():
            return

        df = client.get_dataset_df("DocumentContentDataset")
        assert df.shape == (8, 8)
