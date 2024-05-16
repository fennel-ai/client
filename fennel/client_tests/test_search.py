import unittest
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

import numpy as np
import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import connectors
from fennel.connectors import source
from fennel.datasets import dataset, Dataset, pipeline, field, Count, Sum
from fennel.dtypes import Embedding, oneof, Continuous
from fennel.featuresets import featureset, feature as F, extractor
from fennel.lib import includes, meta, inputs, outputs
from fennel.testing import mock

biq_query = connectors.BigQuery(
    name="bg_source",
    project_id="my-project-356105",
    dataset_id="query_data",
    service_account_key={},
)

s3 = connectors.S3(
    name="s3_source",
    bucket="engagement",
    path_prefix="prod/apac/",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
)

webhook = connectors.Webhook(name="fennel_webhook")

__owner__ = "data-eng@fennel.ai"

################################################################################
#                           Datasets
################################################################################


@meta(owner="e1@company.com")
@source(webhook.endpoint("NotionDocs"), disorder="14d", cdc="append", env="dev")
@source(
    s3.bucket("engagement", prefix="notion"),
    disorder="14d",
    cdc="append",
    every="2m",
    env="prod",
)
@dataset
class NotionDocs:
    doc_id: int
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@source(webhook.endpoint("CodaDocs"), disorder="14d", cdc="append", env="dev")
@source(
    s3.bucket("engagement", prefix="coda"),
    disorder="14d",
    cdc="append",
    env="prod",
)
@dataset
class CodaDocs:
    doc_id: int
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@source(webhook.endpoint("GoogleDocs"), disorder="14d", cdc="append", env="dev")
@source(
    s3.bucket("engagement", prefix="google"),
    disorder="14d",
    cdc="append",
    env="prod",
)
@dataset
class GoogleDocs:
    doc_id: int
    body: str
    title: str
    owner: str
    creation_timestamp: datetime


@dataset
class Document:
    doc_id: int
    body: str
    title: str
    owner: str
    origin: str
    creation_timestamp: datetime

    @pipeline
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


@dataset(index=True)
class DocumentIndexed:
    doc_id: int = field(key=True)
    body: str
    title: str
    owner: str
    origin: str
    creation_timestamp: datetime

    @pipeline
    @inputs(Document)
    def index(cls, ds: Dataset):
        return ds.groupby("doc_id").first()


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


@dataset
class DocumentContentDataset:
    doc_id: int
    bert_embedding: Embedding[128]
    fast_text_embedding: Embedding[256]
    num_words: int
    num_stop_words: int
    top_10_unique_words: List[str]
    creation_timestamp: datetime

    @pipeline
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


@dataset(index=True)
class DocumentContentDatasetIndexed:
    doc_id: int = field(key=True)
    bert_embedding: Embedding[128]
    fast_text_embedding: Embedding[256]
    num_words: int
    num_stop_words: int
    top_10_unique_words: List[str]
    creation_timestamp: datetime

    @pipeline
    @inputs(DocumentContentDataset)
    def index(cls, ds: Dataset):
        return ds.groupby("doc_id").first()


@dataset(index=True)
class TopWordsCount:
    word: str = field(key=True)
    count: int
    timestamp: datetime

    @pipeline
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
                Count(window=Continuous("forever"), into_field="count"),
            ]
        )  # type: ignore


@source(
    webhook.endpoint("UserActivity"), disorder="14d", cdc="append", env="dev"
)
@source(
    biq_query.table("user_activity", cursor="timestamp"),
    every="1h",
    disorder="14d",
    cdc="append",
    env="prod",
)
@dataset
class UserActivity:
    user_id: int
    doc_id: int
    action_type: oneof(str, ["view", "edit"])  # type: ignore
    view_time: float
    timestamp: datetime


@dataset(index=True)
class UserEngagementDataset:
    user_id: int = field(key=True)
    num_views: int
    num_short_views_7d: int
    num_long_views: int
    timestamp: datetime

    @pipeline
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
                Count(
                    window=Continuous("forever"), into_field=str(cls.num_views)
                ),
                Sum(
                    window=Continuous("7d"),
                    of="is_short_click",
                    into_field=str(cls.num_short_views_7d),
                ),
                Sum(
                    window=Continuous("forever"),
                    of="is_long_click",
                    into_field=str(cls.num_long_views),
                ),
            ]
        )


@dataset(index=True)
class DocumentEngagementDataset:
    doc_id: int = field(key=True)
    num_views: int
    num_views_7d: int
    num_views_28d: int
    total_timespent: float
    timestamp: datetime

    @pipeline
    @inputs(UserActivity)
    def doc_engagement_pipeline(cls, ds: Dataset):
        return (
            ds.dedup(by=["user_id", "doc_id"])
            .groupby("doc_id")
            .aggregate(
                [
                    Count(
                        window=Continuous("forever"),
                        into_field=str(cls.num_views),
                    ),
                    Count(
                        window=Continuous("7d"),
                        into_field=str(cls.num_views_7d),
                    ),
                    Count(
                        window=Continuous("28d"),
                        into_field=str(cls.num_views_28d),
                    ),
                    Sum(
                        window=Continuous("forever"),
                        of="view_time",
                        into_field=str(cls.total_timespent),
                    ),
                ]
            )
        )


################################################################################
#                           Featuresets
################################################################################


@featureset
class Query:
    doc_id: int
    user_id: int


@featureset
class UserBehaviorFeatures:
    user_id: int
    num_views: int = F().meta(owner="aditya@fennel.ai")  # type: ignore
    num_short_views_7d: int
    num_long_views: int

    @extractor(deps=[UserEngagementDataset])  # type: ignore
    @inputs(Query.user_id)
    def get_user_features(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserEngagementDataset.lookup(  # type: ignore
            ts, user_id=user_id  # type: ignore
        )
        return df


@featureset
class DocumentFeatures:
    doc_id: int
    num_views: int
    num_views_7d: int
    total_timespent_minutes: float
    num_views_28d: int

    @extractor(deps=[DocumentEngagementDataset])  # type: ignore
    @inputs(Query.doc_id)
    def get_doc_features(cls, ts: pd.Series, doc_id: pd.Series):
        df, found = DocumentEngagementDataset.lookup(  # type: ignore
            ts, doc_id=doc_id  # type: ignore
        )
        df["total_timespent_minutes"] = df["total_timespent"] / 60
        df.drop("total_timespent", axis=1, inplace=True)
        return df


@featureset
class DocumentContentFeatures:
    doc_id: int
    bert_embedding: Embedding[128]
    fast_text_embedding: Embedding[256]
    num_words: int
    num_stop_words: int
    top_10_unique_words: List[str]

    @extractor(deps=[DocumentContentDatasetIndexed])  # type: ignore
    @inputs(Query.doc_id)
    def get_features(cls, ts: pd.Series, doc_id: pd.Series):
        df, found = DocumentContentDatasetIndexed.lookup(  # type: ignore
            ts, doc_id=doc_id  # type: ignore
        )
        df.drop("creation_timestamp", axis=1, inplace=True)
        return df


@featureset
class TopWordsFeatures:
    word: str
    count: int

    @extractor(deps=[TopWordsCount])  # type: ignore
    @inputs("word")
    @outputs("count")
    def get_counts(cls, ts: pd.Series, word: pd.Series):
        df, _ = TopWordsCount.lookup(ts, word=word)  # type: ignore
        df = df.fillna(0)
        df["count"] = df["count"].astype(int)
        return df["count"]


class TestSearchExample(unittest.TestCase):
    def log_document_data(self, client):
        now = datetime.now(timezone.utc)
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
        now = datetime.now(timezone.utc)
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
        client.commit(
            message="Initial commit",
            datasets=[
                NotionDocs,
                CodaDocs,
                GoogleDocs,
                Document,
                DocumentIndexed,
            ],
            env="dev",
        )
        self.log_document_data(client)
        client.sleep()
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)

        doc_ids = pd.DataFrame({"doc_id": [141234, 143354, 33234, 11111]})
        ts = pd.Series([now, now, now, now])
        df, found = client.lookup(
            "DocumentIndexed", keys=doc_ids, timestamps=ts
        )
        assert df.shape == (4, 6)
        assert found.tolist() == [True, True, True, False]

        doc_ids = pd.DataFrame({"doc_id": [141234, 143354, 33234, 11111]})
        ts = pd.Series([yesterday, yesterday, yesterday, yesterday])
        df, found = client.lookup(
            "DocumentIndexed", keys=doc_ids, timestamps=ts
        )
        assert df.shape == (4, 6)
        assert found.tolist() == [False, False, True, False]

    @pytest.mark.integration
    @mock
    def test_search_datasets2(self, client):
        client.commit(
            message="Initial commit",
            datasets=[
                UserActivity,
                UserEngagementDataset,
                DocumentEngagementDataset,
            ],
            env="dev",
        )

        self.log_engagement_data(client)
        client.sleep(20)
        now = datetime.now(timezone.utc)
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
        client.commit(
            message="Initial commit",
            datasets=[
                NotionDocs,
                CodaDocs,
                GoogleDocs,
                Document,
                UserActivity,
                DocumentContentDataset,
                DocumentContentDatasetIndexed,
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
            env="dev",
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
        df = client.query(
            outputs=[
                UserBehaviorFeatures,
                DocumentFeatures,
                DocumentContentFeatures,
            ],
            inputs=[Query.doc_id, Query.user_id],
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
        df = client.query(
            outputs=[TopWordsFeatures.count],
            inputs=[TopWordsFeatures.word],
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
        assert df.shape == (8, 7)
