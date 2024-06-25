import json
import re
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from math import sqrt
from typing import Optional, List, Dict

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.connectors import source, Webhook, ref
from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Dataset,
    LastK,
    Min,
    Max,
    Sum,
    Average,
    Count,
    Stddev,
    Distinct,
    Quantile,
)
from fennel.dtypes import between, oneof, struct, Continuous
from fennel.lib import includes, meta, inputs
from fennel.testing import almost_equal, mock, InternalTestClient, log

################################################################################
#                           Dataset Unit Tests
################################################################################

webhook = Webhook(name="fennel_webhook")

__owner__ = "eng@fennel.ai"


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


@meta(owner="test@test.com")
@dataset(index=True)
class UserInfoDatasetDerived:
    user_id: int = field(key=True, erase_key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    country_name: Optional[str]
    ts: datetime = field(timestamp=True)

    @pipeline
    @inputs(UserInfoDataset)
    def get_info(cls, info: Dataset):
        x = info.rename({"country": "country_name", "timestamp": "ts"})
        return x.drop(columns=["age"])


@meta(owner="test@test.com")
@dataset(index=True)
class UserInfoDatasetDerivedSelect:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    country_name: str
    ts: datetime = field(timestamp=True)

    @pipeline
    @inputs(UserInfoDataset)
    def get_info(cls, info: Dataset):
        x = info.rename({"country": "country_name", "timestamp": "ts"})
        return x.select("user_id", "name", "country_name").dropnull(
            "country_name"
        )


@meta(owner="test@test.com")
@dataset(index=True)
class UserInfoDatasetDerivedDropnull:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    country: str
    age: int
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(UserInfoDataset)
    def get_info(cls, info: Dataset):
        return info.dropnull()


class TestDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_sync_dataset(self, client):
        """Sync the dataset and check if it is synced correctly."""
        # Sync the dataset
        response = client.commit(message="msg", datasets=[UserInfoDataset])
        assert response.status_code == requests.codes.OK, response.json()

    @pytest.mark.integration
    @mock
    def test_simple_log(self, client):
        # Sync the dataset
        client.commit(message="msg", datasets=[UserInfoDataset])
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        sample = client.inspect("UserInfoDataset", n=1)
        assert len(sample) == 1
        sample = client.inspect("UserInfoDataset")
        assert len(sample) == 2

    @mock
    def test_simple_log_mock_log(self, client):
        # Sync the dataset
        client.commit(message="msg", datasets=[UserInfoDataset])
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        log(UserInfoDataset, df)

        sample = client.inspect("UserInfoDataset", n=1)
        assert len(sample) == 1
        sample = client.inspect("UserInfoDataset")
        assert len(sample) == 2

    @pytest.mark.integration
    @mock
    def test_log_stacked_source_keyed_ds(self, client):
        @meta(owner="test@test.com")
        @source(
            webhook.endpoint("UserInfoDataset1"),
            disorder="14d",
            cdc="upsert",
            preproc={"country": "USA"},
        )
        @source(
            webhook.endpoint("UserInfoDataset2"),
            disorder="14d",
            cdc="upsert",
            preproc={"country": "India"},
        )
        @source(
            webhook.endpoint("UserInfoDataset3"),
            disorder="14d",
            cdc="upsert",
            preproc={"country": "China"},
        )
        @dataset(index=True)
        class UserInfoDataset:
            user_id: int = field(key=True).meta(description="User ID")  # type: ignore
            name: str = field().meta(description="User name")  # type: ignore
            age: Optional[int]
            country: str
            timestamp: datetime = field(timestamp=True)

        # Sync the dataset
        client.commit(message="msg", datasets=[UserInfoDataset])
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", yesterday],
            [18234, "Monica", 24, "USA", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset1", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        sample = client.inspect("UserInfoDataset", n=1)
        assert len(sample) == 1
        sample = client.inspect("UserInfoDataset", n=4)
        assert len(sample) == 2

        data = [
            [18232, "Ross", 34, "India", now],
            [17234, "Chandler", 42, "India", now],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset2", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        sample = client.inspect("UserInfoDataset", n=3)
        assert len(sample) == 3
        sample = client.inspect("UserInfoDataset", n=4)
        assert len(sample) == 4

        keys = pd.DataFrame(
            {
                "user_id": [
                    18232,
                    18234,
                    17234,
                ]
            }
        )
        df, found = client.lookup(
            "UserInfoDataset",
            keys=keys,
        )
        assert df["country"].to_list() == ["India", "USA", "India"]
        assert found.tolist() == [True, True, True]

    @pytest.mark.integration
    @mock
    def test_simple_select_rename(self, client):
        client.commit(
            message="msg",
            datasets=[UserInfoDataset],
        )
        client.commit(
            message="add derived dataset",
            datasets=[UserInfoDatasetDerivedSelect],
            incremental=True,
        )
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
            [18235, "Jessica", 24, None, yesterday],
            [18236, "Jane", 24, None, yesterday],
            [18237, "Chandler", 24, None, yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do lookup on UserInfoDataset
        client.sleep()
        ts = pd.Series([now, now])
        user_id_keys = pd.Series([18232, 18234])
        df, found = UserInfoDataset.lookup(ts, user_id=user_id_keys)
        assert df.shape == (2, 5)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["age"].tolist() == [32, 24]
        assert df["country"].tolist() == ["USA", "Chile"]

        # Do lookup on UserInfoDatasetDerived
        df, found = UserInfoDatasetDerivedSelect.lookup(
            ts, user_id=user_id_keys
        )
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["country_name"].tolist() == ["USA", "Chile"]
        # Check if column ts exists
        assert "ts" in df.columns
        assert all(x in df.columns for x in ["user_id", "name", "country_name"])
        assert "age" not in df.columns

    @mock
    def test_simple_select_rename_mock_log(self, client):
        client.commit(
            message="msg",
            datasets=[UserInfoDataset, UserInfoDatasetDerivedSelect],
        )
        client.commit(
            message="add derived dataset",
            datasets=[UserInfoDatasetDerivedSelect],
            incremental=True,
        )
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
            [18235, "Jessica", 24, None, yesterday],
            [18236, "Jane", 24, None, yesterday],
            [18237, "Chandler", 24, None, yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        log(UserInfoDataset, df)

        # Do lookup on UserInfoDataset
        client.sleep()
        ts = pd.Series([now, now])
        user_id_keys = pd.Series([18232, 18234])
        df, found = UserInfoDataset.lookup(ts, user_id=user_id_keys)
        assert df.shape == (2, 5)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["age"].tolist() == [32, 24]
        assert df["country"].tolist() == ["USA", "Chile"]

        # Do lookup on UserInfoDatasetDerived
        df, found = UserInfoDatasetDerivedSelect.lookup(
            ts, user_id=user_id_keys
        )
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["country_name"].tolist() == ["USA", "Chile"]
        # Check if column ts exists
        assert "ts" in df.columns
        assert all(x in df.columns for x in ["user_id", "name", "country_name"])
        assert "age" not in df.columns

    @pytest.mark.integration
    @mock
    def test_simple_drop_null(self, client):
        # Sync the dataset
        response = client.commit(
            message="msg",
            datasets=[UserInfoDataset, UserInfoDatasetDerivedDropnull],
        )
        assert response.status_code == requests.codes.OK, response.json()
        now = datetime.now(timezone.utc)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", None, "Chile", now],
            [18235, "Jessica", 24, None, now],
            [18236, "Jane", 24, None, now],
            [18237, "Chandler", 24, None, now],
            [18238, "Mike", 32, "UK", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        df["age"] = df["age"].astype(pd.Int64Dtype())
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do lookup on UserInfoDataset
        if client.is_integration_client():
            client.sleep()
        ts = pd.Series([now, now, now, now, now, now])
        user_id_keys = pd.Series(
            [18232, 18234, 18235, 18236, 18237, 18238], dtype="Int64"
        )

        df, found = client.lookup(
            "UserInfoDataset",
            timestamps=ts,
            keys=pd.DataFrame({"user_id": user_id_keys}),
        )
        assert df.shape == (6, 5)
        assert df["user_id"].tolist() == [
            18232,
            18234,
            18235,
            18236,
            18237,
            18238,
        ]
        assert df["name"].tolist() == [
            "Ross",
            "Monica",
            "Jessica",
            "Jane",
            "Chandler",
            "Mike",
        ]
        assert df["age"].tolist() == [32, pd.NA, 24, 24, 24, 32]
        assert df["country"].tolist() == [
            "USA",
            "Chile",
            pd.NA,
            pd.NA,
            pd.NA,
            "UK",
        ]

        # Do lookup on UserInfoDatasetDerived
        df, found = client.lookup(
            "UserInfoDatasetDerivedDropnull",
            timestamps=ts,
            keys=pd.DataFrame({"user_id": user_id_keys}),
        )

        assert df.shape == (6, 5)
        assert df["user_id"].tolist() == user_id_keys.tolist()
        assert df["name"].tolist() == [
            "Ross",
            pd.NA,
            pd.NA,
            pd.NA,
            pd.NA,
            "Mike",
        ]
        assert df["country"].tolist() == [
            "USA",
            pd.NA,
            pd.NA,
            pd.NA,
            pd.NA,
            "UK",
        ]

    @pytest.mark.integration
    @mock
    def test_simple_drop_rename(self, client):
        # Sync the dataset
        client.commit(
            message="msg", datasets=[UserInfoDataset, UserInfoDatasetDerived]
        )
        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Do lookup on UserInfoDataset
        if client.is_integration_client():
            client.sleep()
        ts = pd.Series([now, now])
        user_id_keys = pd.Series([18232, 18234])
        df, found = UserInfoDataset.lookup(ts, user_id=user_id_keys)
        assert df.shape == (2, 5)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["age"].tolist() == [32, 24]
        assert df["country"].tolist() == ["USA", "Chile"]

        # Do lookup on UserInfoDatasetDerived
        df, found = UserInfoDatasetDerived.lookup(ts, user_id=user_id_keys)
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["name"].tolist() == ["Ross", "Monica"]
        assert df["country_name"].tolist() == ["USA", "Chile"]
        # Check if column ts exists
        assert "ts" in df.columns
        # Check if column age does not exist
        assert "age" not in df.columns

    @mock
    def test_log_to_dataset(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # Sync the dataset
        client.commit(message="msg", datasets=[UserInfoDataset])

        now = datetime.now(timezone.utc)
        yesterday = now - pd.Timedelta(days=1)
        data = [
            [18232, "Ross", 32, "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        data = [
            [18232, "Ross", "32yrs", "USA", now],
            [18234, "Monica", 24, "Chile", yesterday],
        ]
        df = pd.DataFrame(data, columns=columns)
        if client.is_integration_client():
            response = client.log("fennel_webhook", "UserInfoDataset", df)
            assert response.status_code == requests.codes.SERVER_ERROR
            assert (
                response.json()["error"].strip()
                == """error: trailing characters at line 1 column 3""".strip()
            )
        else:
            with pytest.raises(Exception) as e:
                response = client.log("fennel_webhook", "UserInfoDataset", df)
            assert (
                str(e.value)
                == """Schema validation failed during data insertion to `UserInfoDataset`: Failed to cast data logged to column `age` of type `optional(int)`: Expected bytes, got a 'int' object"""
            )
        client.sleep(10)
        # Do some lookups
        user_ids = pd.Series([18232, 18234, 1920], dtype="Int64")
        lookup_now = datetime.now(timezone.utc) + pd.Timedelta(minutes=1)
        ts = pd.Series([lookup_now, lookup_now, lookup_now])
        df, found = UserInfoDataset.lookup(
            ts,
            user_id=user_ids,
        )
        assert found.tolist() == [True, True, False]
        assert df["name"].tolist() == ["Ross", "Monica", pd.NA]
        assert df["age"].tolist() == [32, 24, pd.NA]
        assert df["country"].tolist() == ["USA", "Chile", pd.NA]
        if not client.is_integration_client():
            assert df["timestamp"].tolist() == [now, yesterday, pd.NaT]
        else:
            df["timestamp"] = df["timestamp"].apply(
                lambda x: x.replace(second=0, microsecond=0)
            )
            now_rounded = now.replace(second=0, microsecond=0)
            yday_rounded = yesterday.replace(second=0, microsecond=0)
            assert df["timestamp"].tolist()[:2] == [
                pd.Timestamp(now_rounded),
                pd.Timestamp(yday_rounded),
            ]
        # Do some lookups with a timestamp
        user_ids = pd.Series([18232, 18234], dtype="Int64")
        six_hours_ago = now - pd.Timedelta(hours=6)
        ts = pd.Series([six_hours_ago, six_hours_ago])
        df, found = UserInfoDataset.lookup(
            ts, user_id=user_ids, fields=["name", "age", "country"]
        )
        assert found.tolist() == [False, True]
        assert df["name"].tolist() == [pd.NA, "Monica"]
        assert df["age"].tolist() == [pd.NA, 24]
        assert df["country"].tolist() == [pd.NA, "Chile"]
        tomorrow = now + pd.Timedelta(days=1)
        three_days_from_now = now + pd.Timedelta(days=3)
        data = [
            [18232, "Ross", 33, "Russia", tomorrow],
            [18234, "Monica", 25, "Columbia", three_days_from_now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK

        client.sleep()

        # Do some lookups
        one_day_from_now = now + pd.Timedelta(days=1)
        three_days_from_now = now + pd.Timedelta(days=3)
        ts = pd.Series([three_days_from_now, one_day_from_now])
        df, found = UserInfoDataset.lookup(
            ts, user_id=user_ids, fields=["name", "age", "country"]
        )
        assert found.tolist() == [True, True]
        assert df.shape == (2, 3)
        assert df["age"].tolist() == [33, 24]
        assert df["country"].tolist() == ["Russia", "Chile"]
        assert df["name"].tolist() == ["Ross", "Monica"]

        ts = pd.Series([three_days_from_now, three_days_from_now])
        df, lookup = UserInfoDataset.lookup(
            ts,
            user_id=user_ids,
            fields=["user_id", "name", "age", "country"],
        )
        assert lookup.tolist() == [True, True]
        assert df.shape == (2, 4)
        assert df["user_id"].tolist() == [18232, 18234]
        assert df["age"].tolist() == [33, 25]
        assert df["country"].tolist() == ["Russia", "Columbia"]

    @mock
    def test_invalid_dataschema(self, client):
        """Check if invalid data raises an error."""
        client.commit(message="msg", datasets=[UserInfoDataset])
        data = [
            [18232, "Ross", "32y", "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        if client.is_integration_client():
            response = client.log("fennel_webhook", "UserInfoDataset", df)
            assert response.status_code == requests.codes.SERVER_ERROR
            assert len(response.json()["error"]) > 0
        else:
            with pytest.raises(Exception):
                response = client.log("fennel_webhook", "UserInfoDataset", df)

    @pytest.mark.integration
    @mock
    def test_deleted_field(self, client):
        with self.assertRaises(Exception) as e:

            @meta(owner="test@test.com")
            @dataset(index=True)
            class UserInfoDataset:
                user_id: int = field(key=True)
                name: str
                age: Optional[int] = field().meta(deleted=True)
                country: Optional[str]
                timestamp: datetime = field(timestamp=True)

            client.commit(message="msg", datasets=[UserInfoDataset])

        assert (
            str(e.exception)
            == "Dataset currently does not support deleted or deprecated fields."
        )


# On demand datasets are not supported for now.

# class TestDocumentDataset(unittest.TestCase):
#     @mock_client
#     def test_log_to_document_dataset(self, client):
#         """Log some data to the dataset and check if it is logged correctly."""
#
#         @meta(owner="aditya@fennel.ai")
#         @dataset
#         class DocumentContentDataset:
#             doc_id: int = field(key=True)
#             bert_embedding: Embedding[4]
#             fast_text_embedding: Embedding[3]
#             num_words: int
#             timestamp: datetime = field(timestamp=True)
#
#             @on_demand(expires_after="3d")
#             @inputs(datetime, int)
#             def get_embedding(cls, ts: pd.Series, doc_ids: pd.Series):
#                 data = []
#                 doc_ids = doc_ids.tolist()
#                 for i in range(len(ts)):
#                     data.append(
#                         [
#                             doc_ids[i],
#                             [0.1, 0.2, 0.3, 0.4],
#                             [1.1, 1.2, 1.3],
#                             10 * i,
#                             ts[i],
#                         ]
#                     )
#                 columns = [
#                     str(cls.doc_id),
#                     str(cls.bert_embedding),
#                     str(cls.fast_text_embedding),
#                     str(cls.num_words),
#                     str(cls.timestamp),
#                 ]
#                 return pd.DataFrame(data, columns=columns), pd.Series(
#                     [True] * len(ts)
#                 )
#
#         # Sync the dataset
#         client.commit(datasets=[DocumentContentDataset])
#         now = datetime.now(timezone.utc)
#         data = [
#             [18232, np.array([1, 2, 3, 4]), np.array([1, 2, 3]), 10, now],
#             [
#                 18234,
#                 np.array([1, 2.2, 0.213, 0.343]),
#                 np.array([0.87, 2, 3]),
#                 9,
#                 now,
#             ],
#             [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
#         ]
#         columns = [
#             "doc_id",
#             "bert_embedding",
#             "fast_text_embedding",
#             "num_words",
#             "timestamp",
#         ]
#         df = pd.DataFrame(data, columns=columns)
#         response = client.log("fennel_webhook","DocumentContentDataset", df)
#         assert response.status_code == requests.codes.OK, response.json()
#
#         # Do some lookups
#         doc_ids = pd.Series([18232, 1728, 18234, 18934, 19200, 91012])
#         ts = pd.Series([now, now, now, now, now, now])
#         df, _ = DocumentContentDataset.lookup(ts, doc_id=doc_ids)
#         assert df.shape == (6, 5)
#         assert df["num_words"].tolist() == [10.0, 9.0, 12, 0, 10.0, 20.0]


################################################################################
#                           Dataset & Pipelines Unit Tests
################################################################################


@meta(owner="test@test.com")
@source(webhook.endpoint("RatingActivity"), disorder="14d", cdc="append")
@source(webhook.endpoint("RatingActivity2"), disorder="14d", cdc="append")
@dataset
class RatingActivity:
    userid: int
    rating: float
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"])  # type: ignore # noqa
    t: datetime


@meta(owner="test@test.com")
@dataset(index=True)
class MovieRatingCalculated:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True, erase_key=True
    )
    rating: float
    num_ratings: int
    sum_ratings: float
    min_ratings: float
    max_ratings: float
    median_ratings: float
    stddev_ratings: float
    distinct_users: List[int]
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_aggregate(cls, activity: Dataset):
        return activity.groupby("movie").aggregate(
            Count(
                window=Continuous("forever"),
                into_field=str(cls.num_ratings),
            ),
            Sum(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.sum_ratings),
            ),
            Average(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.rating),
            ),
            Min(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.min_ratings),
                default=5.0,
            ),
            Max(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.max_ratings),
                default=0.0,
            ),
            Quantile(
                window=Continuous("forever"),
                of="rating",
                p=0.5,
                approx=True,
                into_field=str(cls.median_ratings),
                default=0.0,
            ),
            Stddev(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.stddev_ratings),
            ),
            Distinct(
                window=Continuous("forever"),
                of="userid",
                into_field=str(cls.distinct_users),
                unordered=True,
            ),
        )


@meta(owner="test@test.com")
@source(webhook.endpoint("RatingActivityAlong"), disorder="14d", cdc="append")
@dataset
class RatingActivityAlong:
    userid: int
    rating: float
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"])  # type: ignore # noqa
    t: datetime = field(timestamp=True)
    rating_time: datetime


@meta(owner="test@test.com")
@dataset(index=True)
class MovieRatingCalculatedAlong:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True, erase_key=True
    )
    rating: float
    num_ratings: int
    sum_ratings: float
    min_ratings: float
    max_ratings: float
    median_ratings: float
    stddev_ratings: float
    distinct_users: List[int]
    t: datetime = field(timestamp=True)

    @pipeline
    @inputs(RatingActivityAlong)
    def pipeline_aggregate(cls, activity: Dataset):
        return activity.groupby("movie").aggregate(
            Count(
                window=Continuous("forever"),
                into_field=str(cls.num_ratings),
            ),
            Sum(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.sum_ratings),
            ),
            Average(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.rating),
            ),
            Min(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.min_ratings),
                default=5.0,
            ),
            Max(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.max_ratings),
                default=0.0,
            ),
            Quantile(
                window=Continuous("forever"),
                of="rating",
                p=0.5,
                approx=True,
                into_field=str(cls.median_ratings),
                default=0.0,
            ),
            Stddev(
                window=Continuous("forever"),
                of="rating",
                into_field=str(cls.stddev_ratings),
            ),
            Distinct(
                window=Continuous("forever"),
                of="userid",
                into_field=str(cls.distinct_users),
                unordered=True,
            ),
            along="rating_time",
        )


# Copy of above dataset but can be used as an input to another pipeline.
@meta(owner="test@test.com")
@source(webhook.endpoint("MovieRating"), disorder="14d", cdc="upsert")
@dataset
class MovieRating:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating: between(float, min=0.0, max=5.0)  # type: ignore
    num_ratings: int
    sum_ratings: float
    t: datetime


@meta(owner="test@test.com")
@dataset(index=True)
class MovieRatingTransformed:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating_sq: float
    rating_cube: float
    rating_into_5: float
    rating_orig: float
    t: datetime

    @pipeline
    @inputs(MovieRating)
    def pipeline_transform(cls, m: Dataset):
        def t(df: pd.DataFrame) -> pd.DataFrame:
            df["rating_sq"] = df["rating"] * df["rating"]
            df["rating_cube"] = df["rating_sq"] * df["rating"]
            df["rating_into_5"] = df["rating"] * 5
            return df[
                ["movie", "t", "rating_sq", "rating_cube", "rating_into_5"]
            ]

        x = m.transform(
            t,
            schema={
                "movie": oneof(str, ["Jumanji", "Titanic", "RaOne"]),
                "t": datetime,
                "rating_sq": float,
                "rating_cube": float,
                "rating_into_5": float,
            },
        )

        return x.assign(
            name="rating_orig",
            dtype=float,
            func=lambda df: df["rating_sq"] ** 0.5,
        )


@meta(owner="test@test.com")
@dataset(index=True)
class MovieRatingAssign:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating_sq: float
    rating_cube: float
    rating_into_5: float
    t: datetime

    @pipeline
    @inputs(MovieRating)
    def pipeline_assign(cls, m: Dataset):
        rating_sq = m.assign("rating_sq", float, lambda df: df["rating"] ** 2)
        rating_cube = rating_sq.assign(
            "rating_cube", float, lambda df: df["rating_sq"] * df["rating"]
        )
        rating_into_5 = rating_cube.assign(
            "rating_into_5", float, lambda df: df["rating"] * 5
        )
        return rating_into_5.drop("num_ratings", "sum_ratings", "rating")


class TestBasicTransform(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_transform(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRating, MovieRatingTransformed, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        two_hours_ago = now - timedelta(hours=2)
        data = [
            ["Jumanji", 4, 343, 789, two_hours_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["movie", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()
        # Do some lookups to verify pipeline_transform is working as expected
        an_hour_ago = now - timedelta(hours=1)
        ts = pd.Series([an_hour_ago, an_hour_ago])
        keys = pd.DataFrame({"movie": ["Jumanji", "Titanic"]})
        df, found = client.lookup(
            "MovieRatingTransformed",
            keys=keys,
            timestamps=ts,
        )

        assert found.tolist() == [True, False]
        assert df.shape == (2, 6)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist()[0] == 16
        assert pd.isna(df["rating_sq"].tolist()[1])
        assert df["rating_cube"].tolist()[0] == 64
        assert pd.isna(df["rating_cube"].tolist()[1])
        assert df["rating_into_5"].tolist()[0] == 20
        assert pd.isna(df["rating_into_5"].tolist()[1])
        assert df["rating_orig"].tolist()[0] == 4
        assert pd.isna(df["rating_orig"].tolist()[1])

        client.sleep()
        df, _ = client.lookup(
            "MovieRatingTransformed",
            keys=keys,
        )

        assert df.shape == (2, 6)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, 25]
        assert df["rating_cube"].tolist() == [64, 125]
        assert df["rating_into_5"].tolist() == [20, 25]
        assert df["rating_orig"].tolist() == [4, 5]


@source(
    Webhook(name="webhook").endpoint("Orders"), disorder="14d", cdc="append"
)
@dataset
class Orders:
    uid: int
    skus: List[int]
    prices: List[float]
    timestamp: datetime


@dataset(index=True)
class Derived:
    uid: int = field(key=True)
    sku: int = field(key=True)
    price: Optional[float]
    timestamp: datetime

    @pipeline
    @inputs(Orders)
    def explode_pipeline(cls, ds: Dataset):
        return (
            ds.explode("skus", "prices")
            .rename({"skus": "sku", "prices": "price"})
            .dropnull("sku")
            .groupby("uid", "sku")
            .first()
        )


class TestBasicExplode(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_explode(self, client):
        # # Sync the dataset
        client.commit(message="msg", datasets=[Orders, Derived])
        # log some rows to the transaction dataset
        df = pd.DataFrame(
            [
                {
                    "uid": 1,
                    "skus": [1, 2],
                    "prices": [10.1, 20.0],
                    "timestamp": "2021-01-01T00:00:00",
                },
                {
                    "uid": 2,
                    "skus": [],
                    "prices": [],
                    "timestamp": "2021-01-01T00:00:00",
                },
            ]
        )
        client.log("webhook", "Orders", df)
        client.sleep()

        # do lookup on the WithSquare dataset
        df, found = client.lookup(
            "Derived",
            keys=pd.DataFrame({"uid": [1, 2], "sku": [1, 2]}),
        )
        assert list(found) == [True, False]
        assert df["price"].tolist()[0] == 10.1
        assert pd.isna(df["price"].tolist()[1])


class TestBasicAssign(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_assign(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRating, MovieRatingAssign, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        two_hours_ago = now - timedelta(hours=2)
        data = [
            ["Jumanji", 4, 343, 789, two_hours_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["movie", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        # Do some lookups to verify pipeline_transform is working as expected
        an_hour_ago = now - timedelta(hours=1)
        ts = pd.Series([an_hour_ago, an_hour_ago])
        keys = pd.DataFrame({"movie": ["Jumanji", "Titanic"]})
        df, found = client.lookup(
            "MovieRatingAssign",
            timestamps=ts,
            keys=keys,
        )

        assert found.tolist() == [True, False]
        assert df.shape == (2, 5)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist()[0] == 16
        assert pd.isna(df["rating_sq"].tolist()[1])
        assert df["rating_cube"].tolist()[0] == 64
        assert pd.isna(df["rating_cube"].tolist()[1])
        assert df["rating_into_5"].tolist()[0] == 20
        assert pd.isna(df["rating_into_5"].tolist()[1])

        client.sleep()
        df, _ = client.lookup(
            "MovieRatingAssign",
            keys=keys,
        )

        assert df.shape == (2, 5)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating_sq"].tolist() == [16, 25]
        assert df["rating_cube"].tolist() == [64, 125]
        assert df["rating_into_5"].tolist() == [20, 25]


@meta(owner="test@test.com")
@source(webhook.endpoint("MovieRevenue"), disorder="14d", cdc="upsert")
@dataset(index=True)
class MovieRevenue:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    revenue: int
    t: datetime


@meta(owner="aditya@fennel.ai")
@dataset(index=True)
class MovieStats:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    rating: float
    revenue_in_millions: float
    t: datetime

    @pipeline
    @inputs(MovieRating, MovieRevenue)
    def pipeline_join(cls, rating: Dataset, revenue: Dataset):
        def to_millions(df: pd.DataFrame) -> pd.DataFrame:
            df[str(cls.revenue_in_millions)] = df["revenue"] / 1000000
            df[str(cls.revenue_in_millions)].fillna(-1, inplace=True)
            return df[
                [
                    str(cls.movie),
                    str(cls.t),
                    str(cls.revenue_in_millions),
                    str(cls.rating),
                ]
            ]

        c = rating.join(revenue, how="left", on=[str(cls.movie)])
        # Transform provides additional columns which will be filtered out.
        return c.transform(
            to_millions,
            schema={
                str(cls.movie): oneof(str, ["Jumanji", "Titanic", "RaOne"]),
                str(cls.rating): float,
                str(cls.t): datetime,
                str(cls.revenue_in_millions): float,
            },
        )


class TestBasicJoin(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_join(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRating, MovieRevenue, MovieStats, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        data = [
            ["Jumanji", 4, 343, 789, one_hour_ago],
            ["Titanic", 5, 729, 1232, now],
        ]
        columns = ["movie", "rating", "num_ratings", "sum_ratings", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRating", df)
        assert response.status_code == requests.codes.OK, response.json()

        two_hours_ago = now - timedelta(hours=2)
        data = [["Jumanji", 2000000, two_hours_ago], ["Titanic", 50000000, now]]
        columns = ["movie", "revenue", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieRevenue", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        # Do some lookups to verify pipeline_join is working as expected
        keys = pd.DataFrame({"movie": ["Jumanji", "Titanic"]})
        df, _ = client.lookup(
            "MovieStats",
            keys=keys,
        )
        assert df.shape == (2, 4)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [4, 5]
        assert df["revenue_in_millions"].tolist() == [2, 50]

        # Do some lookup at various timestamps in the past
        ts = pd.Series([two_hours_ago, one_hour_ago, one_hour_ago, now])
        keys = pd.DataFrame(
            {"movie": ["Jumanji", "Jumanji", "Titanic", "Titanic"]}
        )
        df, _ = client.lookup(
            "MovieStats",
            timestamps=ts,
            keys=keys,
        )
        assert df.shape == (4, 4)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Jumanji",
            "Titanic",
            "Titanic",
        ]
        assert pd.isna(df["rating"].tolist()[0])
        assert df["rating"].tolist()[1] == 4
        assert pd.isna(df["rating"].tolist()[2])
        assert df["rating"].tolist()[3] == 5
        assert pd.isna(df["revenue_in_millions"].tolist()[0])
        assert df["revenue_in_millions"].tolist()[1] == 2
        assert pd.isna(df["revenue_in_millions"].tolist()[2])
        assert df["revenue_in_millions"].tolist()[3] == 50


class TestInnerJoinExplodeDedup(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_inner_join_with_explode_dedup(self, client):
        @meta(owner="abhay@fennel.ai")
        @source(webhook.endpoint("MovieInfo"), disorder="14d", cdc="upsert")
        @dataset(index=True)
        class MovieInfo:
            title: str = field(key=True)
            actors: List[str]
            release: datetime

        @meta(owner="abhay@fennel.ai")
        @source(webhook.endpoint("TicketSale"), disorder="14d", cdc="append")
        @dataset
        class TicketSale:
            ticket_id: str
            title: str
            price: int
            at: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset(index=True)
        class ActorStats:
            name: str = field(key=True)
            revenue: int
            at: datetime

            @pipeline
            @inputs(MovieInfo, TicketSale)
            def pipeline_join(cls, info: Dataset, sale: Dataset):
                uniq = sale.dedup(by=["ticket_id"])
                c = (
                    uniq.join(info, how="inner", on=["title"])
                    .explode(columns=["actors"])
                    .rename(columns={"actors": "name"})
                )
                schema = c.schema()
                schema["name"] = str
                c = c.transform(lambda x: x, schema)
                return c.groupby("name").aggregate(
                    Sum(
                        window=Continuous("forever"),
                        of="price",
                        into_field="revenue",
                    ),
                )

        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieInfo, TicketSale, ActorStats],
        )
        data = [
            [
                "Titanic",
                ["Leonardo DiCaprio", "Kate Winslet"],
                datetime.strptime("1997-12-19", "%Y-%m-%d"),
            ],
            [
                "Jumanji",
                ["Robin Williams", "Kirsten Dunst"],
                datetime.strptime("1995-12-15", "%Y-%m-%d"),
            ],
            [
                "Great Gatbsy",
                ["Leonardo DiCaprio", "Carey Mulligan"],
                datetime.strptime("2013-05-10", "%Y-%m-%d"),
            ],
        ]
        columns = ["title", "actors", "release"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieInfo", df)
        assert (
            response.status_code == requests.codes.OK
        ), response.json()  # noqa

        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        one_day_ago = now - timedelta(days=1)
        two_hours_ago = now - timedelta(hours=2)
        columns = ["ticket_id", "title", "price", "at"]
        data = [
            ["1", "Titanic", 50, one_hour_ago],
            ["2", "Titanic", 100, one_day_ago],
            ["3", "Jumanji", 25, one_hour_ago],
            ["4", "The Matrix", 50, two_hours_ago],  # no match
            ["5", "Great Gatbsy", 49, one_hour_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "TicketSale", df)
        assert (
            response.status_code == requests.codes.OK
        ), response.json()  # noqa

        # Do some lookups to verify pipeline_join is working as expected
        keys = pd.DataFrame(
            {"name": ["Leonardo DiCaprio", "Robin Williams", "Keanu Reeves"]}
        )
        client.sleep()
        df, _ = client.lookup(
            "ActorStats",
            keys=keys,
        )
        assert df.shape == (3, 3)
        assert df["name"].tolist() == [
            "Leonardo DiCaprio",
            "Robin Williams",
            "Keanu Reeves",
        ]
        if client.is_integration_client():
            # backend returns default value for the aggregate dataset
            assert df["revenue"].tolist() == [199, 25, 0]
        else:
            assert df["revenue"].tolist() == [199, 25, pd.NA]

        # Now, send the movie info for The Matrix
        columns = ["title", "actors", "release"]
        data = [
            [
                "The Matrix",
                ["Keanu Reeves", "Laurence Fishburne"],
                datetime.strptime("1999-03-31", "%Y-%m-%d"),
            ],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieInfo", df)
        assert (
            response.status_code == requests.codes.OK
        ), response.json()  # noqa

        # Also, update the ticket price for ticket_id 2 to 75
        columns = ["ticket_id", "title", "price", "at"]
        data = [
            ["2", "Titanic", 75, one_day_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "TicketSale", df)
        assert response.status_code == requests.codes.OK

        # Now do the lookup again
        client.sleep()
        client.sleep()
        df, _ = client.lookup("ActorStats", keys=keys)
        assert df.shape == (3, 3)
        assert df["name"].tolist() == [
            "Leonardo DiCaprio",
            "Robin Williams",
            "Keanu Reeves",
        ]
        assert df["revenue"].tolist() == [174, 25, 50]

        # Do some lookup at various timestamps in the past
        three_hours_ago = now - timedelta(hours=3)
        ts = pd.Series([three_hours_ago, three_hours_ago, three_hours_ago])
        df, _ = client.lookup(
            "ActorStats",
            timestamps=ts,
            keys=keys,
        )
        assert df.shape == (3, 3)
        assert df["name"].tolist() == [
            "Leonardo DiCaprio",
            "Robin Williams",
            "Keanu Reeves",
        ]
        if client.is_integration_client():
            # backend returns default values for aggregate datasets
            assert df["revenue"].tolist() == [
                75,
                0,
                0,
            ]
        else:
            assert df["revenue"].tolist() == [
                75,
                pd.NA,
                pd.NA,
            ]


class TestBasicAggregate(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_aggregate(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRatingCalculated, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)

        data = [
            [18231, 2, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 2, "Jumanji", three_hours_ago],
            [18231, 5, "Jumanji", five_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 5, "Titanic", now - timedelta(minutes=1)],
            [18231, 3, "Titanic", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK

        client.sleep()

        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = MovieRatingCalculated.lookup(
            ts,
            movie=names,
        )
        assert df.shape == (2, 10)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]
        assert df["min_ratings"].tolist() == [2, 3]
        assert df["max_ratings"].tolist() == [5, 5]
        assert all(
            [
                abs(actual - expected) < 0.001
                for actual, expected in zip(
                    df["stddev_ratings"].tolist(), [sqrt(3 / 2), sqrt(4 / 5)]
                )
            ]
        )
        assert [round(x) for x in df["median_ratings"]] == [3, 4]
        assert df["distinct_users"].tolist() == [[18231], [18231]]


class TestBasicAggregateAlong(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_aggregate_along(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRatingCalculatedAlong, RatingActivityAlong],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)

        data = [
            [18231, 2, "Jumanji", five_hours_ago, five_hours_ago],
            [18231, 3, "Jumanji", five_hours_ago, four_hours_ago],
            [18231, 2, "Jumanji", five_hours_ago, three_hours_ago],
            [18231, 5, "Jumanji", five_hours_ago, five_hours_ago],
            [18231, 4, "Titanic", five_hours_ago, three_hours_ago],
            [18231, 3, "Titanic", five_hours_ago, two_hours_ago],
            [18231, 5, "Titanic", five_hours_ago, one_hour_ago],
            [18231, 5, "Titanic", five_hours_ago, now - timedelta(minutes=1)],
            [18231, 3, "Titanic", five_hours_ago, two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t", "rating_time"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivityAlong", df)
        assert response.status_code == requests.codes.OK

        client.sleep()

        # Do some lookups back in time to verify pipeline_aggregate is working as expected
        ts = pd.Series([four_hours_ago, three_hours_ago])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = client.lookup(
            MovieRatingCalculatedAlong,
            timestamps=ts,
            keys=pd.DataFrame({"movie": names}),
        )
        assert df.shape == (2, 10)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        if client.is_integration_client():
            assert df["rating"].tolist() == [3, 4]
        else:
            assert df["rating"].tolist() == [pytest.approx(3.33333333), 4]
        assert df["num_ratings"].tolist() == [3, 1]
        assert df["sum_ratings"].tolist() == [10.0, 4.0]
        assert df["min_ratings"].tolist() == [2, 4]
        assert df["max_ratings"].tolist() == [5, 4]
        assert df["stddev_ratings"].tolist() == [pytest.approx(1.247219), 0]
        assert [round(x) for x in df["median_ratings"]] == [3, 4]
        assert df["distinct_users"].tolist() == [[18231], [18231]]

        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series([now, now])
        names = pd.Series(["Jumanji", "Titanic"])
        df, _ = client.lookup(
            MovieRatingCalculatedAlong,
            timestamps=ts,
            keys=pd.DataFrame({"movie": names}),
        )
        assert df.shape == (2, 10)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]
        assert df["min_ratings"].tolist() == [2, 3]
        assert df["max_ratings"].tolist() == [5, 5]
        assert all(
            [
                abs(actual - expected) < 0.001
                for actual, expected in zip(
                    df["stddev_ratings"].tolist(), [sqrt(3 / 2), sqrt(4 / 5)]
                )
            ]
        )
        assert [round(x) for x in df["median_ratings"]] == [3, 4]
        assert df["distinct_users"].tolist() == [[18231], [18231]]

        # Do some lookups online to verify pipeline_aggregate is working as expected
        keys = pd.DataFrame({"movie": ["Jumanji", "Titanic"]})
        df, _ = client.lookup(MovieRatingCalculatedAlong, keys=keys)
        assert df.shape == (2, 10)
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert df["rating"].tolist() == [3, 4]
        assert df["num_ratings"].tolist() == [4, 5]
        assert df["sum_ratings"].tolist() == [12, 20]
        assert df["min_ratings"].tolist() == [2, 3]
        assert df["max_ratings"].tolist() == [5, 5]
        assert all(
            [
                abs(actual - expected) < 0.001
                for actual, expected in zip(
                    df["stddev_ratings"].tolist(), [sqrt(3 / 2), sqrt(4 / 5)]
                )
            ]
        )
        assert [round(x) for x in df["median_ratings"]] == [3, 4]
        assert df["distinct_users"].tolist() == [[18231], [18231]]


@meta(owner="test@test.com")
@dataset(index=True)
class MovieRatingWindowed:
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    num_ratings_3d: int
    sum_ratings_7d: float
    avg_rating_6h: float
    total_ratings: int
    std_rating_3d: float
    std_rating_7d: float
    std_rating_10m: float
    std_rating_10m_other_default: float

    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_aggregate(cls, activity: Dataset):
        return activity.groupby("movie").aggregate(
            Count(window=Continuous("3d"), into_field=str(cls.num_ratings_3d)),
            Sum(
                window=Continuous("7d"),
                of="rating",
                into_field=str(cls.sum_ratings_7d),
            ),
            Average(
                window=Continuous("6h"),
                of="rating",
                into_field=str(cls.avg_rating_6h),
            ),
            Count(
                window=Continuous("forever"), into_field=str(cls.total_ratings)
            ),
            Stddev(
                window=Continuous("3d"),
                of="rating",
                into_field=str(cls.std_rating_3d),
            ),
            Stddev(
                window=Continuous("7d"),
                of="rating",
                into_field=str(cls.std_rating_7d),
            ),
            Stddev(
                window=Continuous("10m"),
                of="rating",
                into_field=str(cls.std_rating_10m),
            ),
            Stddev(
                window=Continuous("10m"),
                of="rating",
                default=-3.14159,
                into_field=str(cls.std_rating_10m_other_default),
            ),
        )


class TestBasicWindowAggregate(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_window_aggregate(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRatingWindowed, RatingActivity],
        )
        true_now = datetime.now(timezone.utc)
        now = true_now - timedelta(days=10) - timedelta(minutes=1)
        three_hours = now + timedelta(hours=3)
        six_hours = now + timedelta(hours=6)
        one_day = now + timedelta(days=1)
        two_days = now + timedelta(days=2)
        three_days = now + timedelta(days=3)
        six_days = now + timedelta(days=6)
        seven_days = now + timedelta(days=7)
        data = [
            [18231, 2, "Jumanji", three_hours],
            [18231, 3, "Jumanji", six_hours],
            [18231, 2, "Jumanji", one_day],
            [18231, 5, "Jumanji", three_days],
            [18231, 4, "Jumanji", six_days],
            [18231, 4, "Jumanji", seven_days],
            [18231, 4, "Titanic", one_day],
            [18231, 3, "Titanic", two_days],
            [18231, 5, "Titanic", three_days],
            [18231, 5, "Titanic", two_days],
            [18231, 3, "Titanic", six_days],
            [18231, 3, "Titanic", seven_days],
            [18921, 1, "Jumanji", true_now],
            [18921, 1, "Titanic", true_now],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        now = true_now - timedelta(days=10)
        eight_days = now + timedelta(days=8)
        five_days = now + timedelta(days=5)
        one_day = now + timedelta(days=1)
        # Do some lookups to verify pipeline_aggregate is working as expected
        ts = pd.Series(
            [eight_days, eight_days, five_days, five_days, one_day, one_day]
        )
        keys = pd.DataFrame(
            {
                "movie": [
                    "Jumanji",
                    "Titanic",
                    "Jumanji",
                    "Titanic",
                    "Jumanji",
                    "Titanic",
                ]
            }
        )
        df, _ = client.lookup("MovieRatingWindowed", keys=keys, timestamps=ts)
        assert df.shape == (6, 10)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Titanic",
            "Jumanji",
            "Titanic",
            "Jumanji",
            "Titanic",
        ]
        assert df["num_ratings_3d"].tolist() == [2, 2, 1, 1, 3, 1]

        assert df["sum_ratings_7d"].tolist() == [13, 19, 12, 17, 7, 4]
        assert df["avg_rating_6h"].tolist() == [0.0, 0.0, 0.0, 0.0, 2.0, 4.0]
        assert df["total_ratings"].tolist() == [6, 6, 4, 4, 3, 1]
        assert all(
            [
                almost_equal(actual, expected)
                for actual, expected in zip(
                    df["std_rating_3d"].tolist(), [0, 0, 0, 0, sqrt(2) / 3, 0]
                )
            ]
        )
        assert all(
            [
                almost_equal(actual, expected)
                for actual, expected in zip(
                    df["std_rating_7d"].tolist(),
                    [
                        sqrt(2 / 9),
                        sqrt(0.96),
                        sqrt(1.5),
                        sqrt(11 / 16),
                        sqrt(2 / 9),
                        0,
                    ],
                )
            ]
        )
        assert all(
            [
                almost_equal(actual, expected)
                for actual, expected in zip(
                    df["std_rating_10m"].tolist(),
                    [
                        -1,
                        -1,
                        -1,
                        -1,
                        0,
                        0,
                    ],
                )
            ]
        ), f"expected [-1, -1, -1, 0, 0,] got {df['std_rating_10m'].tolist()}"
        assert all(
            [
                almost_equal(actual, expected)
                for actual, expected in zip(
                    df["std_rating_10m_other_default"].tolist(),
                    [
                        -3.14159,
                        -3.14159,
                        -3.14159,
                        -3.14159,
                        0,
                        0,
                    ],
                )
            ]
        )

    @pytest.mark.integration
    @mock
    def test_basic_aggregate_with_erase(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MovieRatingCalculated, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)

        data = [
            [18231, 2, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 2, "Jumanji", three_hours_ago],
            [18231, 5, "Jumanji", five_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 5, "Titanic", now - timedelta(minutes=1)],
            [18231, 3, "Titanic", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK

        client.sleep()
        response = client.erase(
            MovieRatingCalculated, pd.DataFrame({"movie": ["Jumanji"]})
        )
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        client.sleep()
        client.sleep()
        client.sleep()
        client.sleep()

        # Do some lookups to verify pipeline_aggregate is working as expected
        df, found = client.lookup(
            "MovieRatingCalculated",
            pd.DataFrame({"movie": ["Jumanji", "Titanic"]}),
        )
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert found.to_list() == [False, True]

        ts = pd.Series([now, now])
        df, found = client.lookup(
            "MovieRatingCalculated",
            pd.DataFrame({"movie": ["Jumanji", "Titanic"]}),
            timestamps=ts,
        )
        assert df["movie"].tolist() == ["Jumanji", "Titanic"]
        assert found.to_list() == [False, True]


@meta(owner="test@test.com")
@dataset(index=True)
class PositiveRatingActivity:
    cnt_rating: int
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def filter_positive_ratings(cls, rating: Dataset):
        filtered_ds = rating.filter(lambda df: df["rating"] >= 3.5)
        filter2 = filtered_ds.filter(
            lambda df: df["movie"].isin(["Jumanji", "Titanic", "RaOne"])
        )
        return filter2.groupby("movie").aggregate(
            Count(window=Continuous("forever"), into_field=str(cls.cnt_rating)),
        )


class TestBasicFilter(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_filter(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[PositiveRatingActivity, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 3.5, "Jumanji", three_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 4, "Titanic", minute_ago],
            [18231, 2, "RaOne", one_hour_ago],
            [18231, 3, "RaOne", minute_ago],
            [18231, 1, "RaOne", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        # Do some lookups to verify pipeline_aggregate is working as expected
        df, _ = client.lookup(
            "PositiveRatingActivity",
            keys=pd.DataFrame({"movie": ["Jumanji", "Titanic", "RaOne"]}),
        )
        assert df.shape == (3, 3)
        assert df["movie"].tolist() == ["Jumanji", "Titanic", "RaOne"]
        if client.is_integration_client():
            # backend returns default values for aggregate dataset
            assert df["cnt_rating"].tolist() == [2, 3, 0]
        else:
            assert df["cnt_rating"].tolist() == [2, 3, pd.NA]

        ts = pd.Series([two_hours_ago, two_hours_ago, two_hours_ago])
        df, _ = client.lookup(
            "PositiveRatingActivity",
            keys=pd.DataFrame({"movie": ["Jumanji", "Titanic", "RaOne"]}),
            timestamps=ts,
        )
        assert df.shape == (3, 3)
        assert df["movie"].tolist() == ["Jumanji", "Titanic", "RaOne"]
        if client.is_integration_client():
            # backend returns default values for aggregate dataset
            assert df["cnt_rating"].tolist() == [2, 1, 0]
        else:
            assert df["cnt_rating"].tolist() == [2, 1, pd.NA]


@meta(owner="test@test.com")
@dataset(index=True)
class UniqueMoviesSeen:
    static: str = field(key=True)
    unique_movies: int
    unique_movies_2h: int
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_unique_movies_seen(cls, rating: Dataset):
        schema = rating.schema()
        schema["static"] = str
        rating_with_static_col = rating.transform(
            lambda df: df.assign(
                static="static",
            ),
            schema,
        )
        return rating_with_static_col.groupby("static").aggregate(
            Count(
                window=Continuous("forever"),
                into_field=str(cls.unique_movies),
                of="movie",
                unique=True,
                approx=True,
            ),
            Count(
                window=Continuous("2h"),
                into_field=str(cls.unique_movies_2h),
                of="movie",
                unique=True,
                approx=True,
            ),
        )


class TestBasicCountUnique(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_count_unique(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[UniqueMoviesSeen, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 3.5, "Jumanji", three_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 4, "Titanic", minute_ago],
            [18231, 2, "RaOne", one_hour_ago],
            [18231, 3, "RaOne", minute_ago],
            [18231, 1, "RaOne", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        # Do some lookups to verify pipeline_unique_movies_seen is working as expected
        df, _ = client.lookup(
            "UniqueMoviesSeen",
            keys=pd.DataFrame({"static": ["static"]}),
        )
        assert df.shape == (1, 4)
        assert df["unique_movies"].tolist() == [3]
        assert df["unique_movies_2h"].tolist() == [2]

        two_and_half_hours_ago = now - timedelta(hours=2, minutes=30)
        six_hours_ago = now - timedelta(hours=6)
        ts = pd.Series([two_and_half_hours_ago, four_hours_ago, six_hours_ago])
        df, _ = client.lookup(
            "UniqueMoviesSeen",
            timestamps=ts,
            keys=pd.DataFrame({"static": ["static", "static", "static"]}),
        )
        assert df.shape == (3, 4)
        if client.is_integration_client():
            # Our backends return default values for the aggregate datasets
            assert df["unique_movies"].tolist() == [2, 1, 0]
            assert df["unique_movies_2h"].tolist() == [2, 1, 0]
        else:
            assert df["unique_movies"].tolist() == [2, 1, pd.NA]
            assert df["unique_movies_2h"].tolist() == [2, 1, pd.NA]


@meta(owner="test@test.com")
@dataset(index=True)
class UserUniqueMoviesSeen:
    userid: int = field(key=True)
    unique_movies: List[str]
    unique_movies_2h: List[str]
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_unique_movies_seen(cls, rating: Dataset):
        schema = rating.schema()
        schema["movie"] = str
        rating_t = rating.transform(lambda df: df, schema)
        return rating_t.groupby("userid").aggregate(
            Distinct(
                window=Continuous("forever"),
                into_field=str(cls.unique_movies),
                of="movie",
                unordered=True,
            ),
            Distinct(
                window=Continuous("2h"),
                into_field=str(cls.unique_movies_2h),
                of="movie",
                unordered=True,
            ),
        )


class TestBasicDistinct(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_basic_distinct(self, client):
        client.commit(
            message="msg",
            datasets=[UserUniqueMoviesSeen, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)
        three_hours_ago = now - timedelta(hours=3)
        four_hours_ago = now - timedelta(hours=4)
        five_hours_ago = now - timedelta(hours=5)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", five_hours_ago],
            [18231, 3, "Jumanji", four_hours_ago],
            [18231, 3.5, "Jumanji", three_hours_ago],
            [18231, 4, "Titanic", three_hours_ago],
            [18231, 3, "Titanic", two_hours_ago],
            [18231, 5, "Titanic", one_hour_ago],
            [18231, 4, "Titanic", minute_ago],
            [18231, 2, "RaOne", one_hour_ago],
            [18231, 3, "RaOne", minute_ago],
            [18231, 1, "RaOne", two_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        # Do some lookups to verify pipeline_unique_movies_seen is working as expected
        ts = pd.Series([now])
        df, _ = UserUniqueMoviesSeen.lookup(
            ts,
            userid=pd.Series([18231]),
        )
        assert df.shape == (1, 4)
        assert sorted(df["unique_movies"].tolist()[0]) == [
            "Jumanji",
            "RaOne",
            "Titanic",
        ]

        assert sorted(df["unique_movies_2h"].tolist()[0]) == [
            "RaOne",
            "Titanic",
        ]


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class LastMovieSeen:
    userid: int = field(key=True)
    rating: float
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"])  # type: ignore # noqa
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_last_movie_seen(cls, rating: Dataset):
        return rating.groupby("userid").latest()


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class NumLastMovieSeenByUser:
    userid: int = field(key=True)
    # Count should always be 1
    cnt: int
    t: datetime

    @pipeline
    @inputs(LastMovieSeen)
    def pipeline_last_movie_seen(cls, rating: Dataset):
        return rating.groupby("userid").aggregate(
            cnt=Count(window=Continuous("forever")),
        )


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class NumTimesLastMovie:
    """
    Given a movie, count the number of times it was the last movie seen by a user
    """

    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    count: int
    t: datetime

    @pipeline
    @inputs(LastMovieSeen)
    def pipeline_num_last(cls, first_movies: Dataset):
        return first_movies.groupby("movie").aggregate(
            Count(
                window=Continuous("forever"),
                into_field=str(cls.count),
            )
        )


class TestLastOp(unittest.TestCase):
    @mock
    def test_intermediate_log(self, client):
        client.commit(
            message="msg",
            datasets=[
                LastMovieSeen,
                RatingActivity,
                NumTimesLastMovie,
                NumLastMovieSeenByUser,
            ],
        )
        now = datetime.now(timezone.utc)
        log(
            LastMovieSeen,
            pd.DataFrame(
                {
                    "userid": [1, 2, 3],
                    "rating": [4.5, 3, 5],
                    "movie": ["Jumanji", "Titanic", "Titanic"],
                    "t": [now, now, now],
                }
            ),
        )

        df, found = client.lookup(
            NumLastMovieSeenByUser, keys=pd.DataFrame({"userid": [1, 2, 3]})
        )
        assert df.shape == (3, 3)
        assert df["cnt"].tolist() == [1, 1, 1]

    @pytest.mark.integration
    @mock
    def test_last_op(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[
                LastMovieSeen,
                RatingActivity,
                NumTimesLastMovie,
                NumLastMovieSeenByUser,
            ],
        )
        now = datetime.now(timezone.utc)
        five_hours_ago = now - timedelta(hours=5)
        three_hours_ago = now - timedelta(hours=3)
        two_hours_ago = now - timedelta(hours=2)
        one_hour_ago = now - timedelta(hours=2)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", five_hours_ago],
            [18232, 3, "Titanic", five_hours_ago],
            [18233, 5, "Titanic", five_hours_ago],
            [18234, 4, "RaOne", five_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        # Do some lookups to verify pipeline_first_movie_seen is working as expected
        ts = pd.Series([now, now, now, now])
        df, _ = LastMovieSeen.lookup(
            ts,
            userid=pd.Series([18231, 18232, 18233, 18234]),
        )
        assert df.shape == (4, 4)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Titanic",
            "Titanic",
            "RaOne",
        ]
        assert df["rating"].tolist() == [4.5, 3, 5, 4]

        # Now, log some more data at later times. We should still get the same results as before.
        data = [
            [18232, 3, "Titanic", two_hours_ago + timedelta(minutes=1)],
            [18231, 3.5, "Jumanji", three_hours_ago],
            [18234, 4, "Titanic", minute_ago],
            [18231, 2, "RaOne", one_hour_ago],
            [18233, 3, "RaOne", minute_ago],
            [18232, 1, "RaOne", two_hours_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        # Do some lookups to verify pipeline_first_movie_seen is working as expected
        ts = pd.Series([now, now, now, now])
        df, _ = LastMovieSeen.lookup(
            ts,
            userid=pd.Series([18231, 18232, 18233, 18234]),
        )
        assert df.shape == (4, 4)
        assert df["movie"].tolist() == [
            "RaOne",
            "Titanic",
            "RaOne",
            "Titanic",
        ]
        assert df["rating"].tolist() == [2.0, 3.0, 3.0, 4.0]

        # Do some lookups to verify pipeline_num_last is working as expected
        ts = pd.Series([now, now, now])
        df, _ = NumTimesLastMovie.lookup(
            ts,
            movie=pd.Series(["Jumanji", "Titanic", "RaOne"]),
        )
        assert df.shape == (3, 3)
        assert df["count"].tolist() == [0, 2, 2]

        ts = pd.Series([now, now, now])
        df, _ = NumLastMovieSeenByUser.lookup(
            ts,
            userid=pd.Series([18231, 18232, 18233]),
        )
        assert df.shape == (3, 3)
        assert df["cnt"].tolist() == [1, 1, 1]


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class FirstMovieSeen:
    userid: int = field(key=True)
    rating: float
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"])  # type: ignore # noqa
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_first_movie_seen(cls, rating: Dataset):
        return rating.groupby("userid").first()


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class NumTimesFirstMovie:
    """
    Given a movie, count the number of times it was the first movie seen by a user
    """

    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"]) = field(  # type: ignore
        key=True
    )
    count: int
    t: datetime

    @pipeline
    @inputs(FirstMovieSeen)
    def pipeline_num_first(cls, first_movies: Dataset):
        return first_movies.groupby("movie").aggregate(
            Count(
                window=Continuous("forever"),
                into_field=str(cls.count),
            )
        )


class TestFirstOp(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_first_op(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[FirstMovieSeen, RatingActivity, NumTimesFirstMovie],
        )
        now = datetime.now(timezone.utc)
        five_hours_ago = now - timedelta(hours=5)
        three_hours_ago = now - timedelta(hours=3)
        two_hours_ago = now - timedelta(hours=2)
        one_hour_ago = now - timedelta(hours=2)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", five_hours_ago],
            [18232, 3, "Titanic", five_hours_ago],
            [18233, 5, "Titanic", five_hours_ago],
            [18234, 4, "RaOne", five_hours_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        # Do some lookups to verify pipeline_first_movie_seen is working as expected
        ts = pd.Series([now, now, now, now])
        df, _ = FirstMovieSeen.lookup(
            ts,
            userid=pd.Series([18231, 18232, 18233, 18234]),
        )
        assert df.shape == (4, 4)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Titanic",
            "Titanic",
            "RaOne",
        ]
        assert df["rating"].tolist() == [4.5, 3, 5, 4]

        # Now, log some more data at later times. We should still get the same results as before.
        data = [
            [18232, 3, "Titanic", two_hours_ago],
            [18231, 3.5, "Jumanji", three_hours_ago],
            [18234, 4, "Titanic", minute_ago],
            [18231, 2, "RaOne", one_hour_ago],
            [18233, 3, "RaOne", minute_ago],
            [18232, 1, "RaOne", two_hours_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        # Do some lookups to verify pipeline_first_movie_seen is working as expected
        ts = pd.Series([now, now, now, now])
        df, _ = FirstMovieSeen.lookup(
            ts,
            userid=pd.Series([18231, 18232, 18233, 18234]),
        )
        assert df.shape == (4, 4)
        assert df["movie"].tolist() == [
            "Jumanji",
            "Titanic",
            "Titanic",
            "RaOne",
        ]
        assert df["rating"].tolist() == [4.5, 3, 5, 4]

        # Following does not work with mock client.
        # Do some lookups to verify pipeline_num_first is working as expected
        ts = pd.Series([now, now, now])
        df, _ = NumTimesFirstMovie.lookup(
            ts,
            movie=pd.Series(["Jumanji", "Titanic", "RaOne"]),
        )
        assert df.shape == (3, 3)
        assert df["count"].tolist() == [1, 2, 1]


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class FirstMovieSeenWithFilter:
    userid: int = field(key=True)
    rating: float
    movie: oneof(str, ["Jumanji", "Titanic", "RaOne"])  # type: ignore # noqa
    t: datetime

    @pipeline
    @inputs(RatingActivity)
    def pipeline_first_movie_seen(cls, rating: Dataset):
        filtered_rating = rating.filter(lambda df: df["rating"] <= 3)
        return filtered_rating.groupby("userid").first()


class TestWaterMark(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_water_marking(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[FirstMovieSeenWithFilter, RatingActivity],
        )
        now = datetime.now(timezone.utc)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [18231, 4.5, "Jumanji", minute_ago],
        ]
        columns = ["userid", "rating", "movie", "t"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()
        # Do some lookups to verify pipeline_first_movie_seen is working as expected
        ts = pd.Series([now])
        df, _ = client.lookup(
            FirstMovieSeenWithFilter,
            timestamps=ts,
            keys=pd.DataFrame({"userid": [18231]}),
        )
        assert df.shape == (1, 4)
        assert df["movie"].tolist() == [pd.NA]
        assert df["rating"].tolist() == [pd.NA]

        # Now log several data points from sixteen days ago
        sixteen_days_ago = now - timedelta(days=16)
        data = [
            [18231, 1, "Jumanji", sixteen_days_ago],
            [18231, 1, "Jumanji", sixteen_days_ago],
            [18231, 1, "Jumanji", sixteen_days_ago],
            [18231, 1, "Jumanji", sixteen_days_ago],
            [18231, 1, "Jumanji", sixteen_days_ago],
            [18231, 1, "Jumanji", sixteen_days_ago],
            [18232, 3, "Titanic", sixteen_days_ago],
            [18233, 5, "Titanic", sixteen_days_ago],
            [18232, 3, "Titanic", sixteen_days_ago],
            [18233, 5, "Titanic", sixteen_days_ago],
            [18232, 3, "RaOne", sixteen_days_ago],
            [18233, 5, "RaOne", sixteen_days_ago],
            [18232, 3, "RaOne", sixteen_days_ago],
            [18233, 5, "RaOne", sixteen_days_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep(10)

        # Ensure that the data is actually from sixteen days ago
        df, _ = FirstMovieSeenWithFilter.lookup(ts, userid=pd.Series([18231]))
        assert df.shape == (1, 4)
        assert df["movie"].tolist() == [
            "Jumanji",
        ]
        assert df["rating"].tolist() == [1]

        if not client.is_integration_client():
            return
        # The remainder test will only work with integration client
        # that maintains watermarks.

        # Now log data 40 days ago
        forty_days_ago = now - timedelta(days=40)
        data = [
            [18231, 2, "Jumanji", forty_days_ago],
            [18231, 2, "Jumanji", forty_days_ago],
            [18233, 3, "Titanic", forty_days_ago],
            [18233, 3, "Titanic", forty_days_ago],
            [18233, 3, "RaOne", forty_days_ago],
            [18233, 3, "RaOne", forty_days_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "RatingActivity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep(10)

        # Ensure that the data is ONLY from sixteen days ago since watermark
        # wont allow data from 40 days ago to be processed
        df, _ = FirstMovieSeenWithFilter.lookup(ts, userid=pd.Series([18231]))
        assert df.shape == (1, 5)
        assert df["movie"].tolist() == [
            "Jumanji",
        ]
        assert df["rating"].tolist() == [1]


################################################################################
#                           Dataset Complex Structure Unit Tests
################################################################################


@struct
class Manufacturer:
    name: str
    country: str


@struct
class Car:
    make: Manufacturer
    model: str
    year: int


@meta(owner="test@test.com")
@source(webhook.endpoint("DealerDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class Dealer:
    name: str = field(key=True)
    address: str
    cars: List[Car]
    timestamp: datetime


@meta(owner="test@tesst.com")
@dataset(index=True)
class DealerNumCars:
    name: str = field(key=True)
    num_cars: int
    timestamp: datetime

    @pipeline
    @inputs(Dealer)
    def pipeline_dealer_num_cars(cls, dealer: Dataset):
        schema = dealer.schema()
        schema["num_cars"] = int
        dealer = dealer.transform(  # type: ignore
            lambda df: df.assign(
                num_cars=df["cars"].apply(len),
            ),
            schema,
        )
        schema.pop("address")
        schema.pop("cars")
        # Drop columns we don't need
        return dealer.transform(
            lambda df: df.drop(columns=["address", "cars"]),
            schema,
        )


class TestNestedStructType(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_nested_struct_type(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[DealerNumCars, Dealer],
        )
        now = datetime.now(timezone.utc) - timedelta(hours=1)
        data = {
            "name": ["Test Dealer", "Second Dealer", "Third Dealer"],
            "address": [
                "123 Test Street",
                "456 Second Street",
                "789 Third Street",
            ],
            "cars": [
                [
                    {
                        "make": {
                            "name": "Test Manufacturer",
                            "country": "Test Country",
                        },
                        "model": "Test Model",
                        "year": 2023,
                    }
                ],
                [
                    {
                        "make": {
                            "name": "Second Manufacturer",
                            "country": "Second Country",
                        },
                        "model": "Second Model",
                        "year": 2024,
                    }
                ],
                [
                    {
                        "make": {
                            "name": "Third Manufacturer",
                            "country": "Third Country",
                        },
                        "model": "Third Model",
                        "year": 2025,
                    },
                    {
                        "make": {
                            "name": "Third Manufacturer 2",
                            "country": "Third Country",
                        },
                        "model": "Third Model",
                        "year": 2026,
                    },
                ],
            ],
            "timestamp": [now, now, now],
        }
        df = pd.DataFrame(data)
        # Log the dealer data
        response = client.log("fennel_webhook", "DealerDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        now = datetime.now(timezone.utc)
        # Verify the data by looking it up
        df, _ = Dealer.lookup(
            pd.Series([now, now]),
            name=pd.Series(["Test " "Dealer", "Third Dealer"]),
        )
        assert df.shape == (2, 4)
        assert df["name"].tolist() == ["Test Dealer", "Third Dealer"]
        assert df["address"].tolist() == ["123 Test Street", "789 Third Street"]
        assert len(df["cars"].tolist()) == 2

        manufacturer1 = df["cars"].tolist()[0][0]
        assert manufacturer1.make.name == "Test Manufacturer"
        assert manufacturer1.make.country == "Test Country"
        assert manufacturer1.model == "Test Model"
        assert manufacturer1.year == 2023

        manufacturer2 = df["cars"].tolist()[1][0]
        assert manufacturer2.make.name == "Third Manufacturer"
        assert manufacturer2.make.country == "Third Country"
        assert manufacturer2.model == "Third Model"
        assert manufacturer2.year == 2025

        # Lookup the DealerNumCars dataset
        df, _ = DealerNumCars.lookup(
            pd.Series([now, now]),
            name=pd.Series(["Test Dealer", "Third Dealer"]),
        )
        assert df.shape == (2, 3)
        assert df["name"].tolist() == ["Test Dealer", "Third Dealer"]
        assert df["num_cars"].tolist() == [1, 2]


################################################################################
#                           Dataset & Pipelines Complex Unit Tests
################################################################################


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("Activity"), disorder="14d", cdc="append")
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="me@fenne.ai")
@source(webhook.endpoint("MerchantInfo"), disorder="14d", cdc="upsert")
@dataset(index=True, history="4m")
class MerchantInfo:
    merchant_id: int = field(key=True)
    category: str
    location: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset(index=True)
class FraudReportAggregatedDataset:
    category: str = field(key=True)
    timestamp: datetime

    num_categ_fraudulent_transactions: int
    num_categ_fraudulent_transactions_7d: int
    sum_categ_fraudulent_transactions_7d: float

    @pipeline
    @inputs(Activity, MerchantInfo)
    def create_fraud_dataset(cls, activity: Dataset, merchant_info: Dataset):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df_timestamp = pd.concat([df_json, df["timestamp"]], axis=1)
            return df_timestamp

        def fillna(df: pd.DataFrame) -> pd.DataFrame:
            df["category"] = df["category"].fillna("unknown")
            df["location"] = df["location"].fillna("unknown")
            return df

        filtered_ds = activity.filter(lambda df: df["action_type"] == "report")
        ds = filtered_ds.transform(
            extract_info,
            schema={
                "transaction_amount": float,
                "merchant_id": int,
                "timestamp": datetime,
            },
        )
        ds = ds.join(
            merchant_info,
            how="inner",
            on=["merchant_id"],
        )
        return ds.groupby("category").aggregate(
            [
                Count(
                    window=Continuous("forever"),
                    into_field=str(cls.num_categ_fraudulent_transactions),
                ),
                Count(
                    window=Continuous("1w"),
                    into_field=str(cls.num_categ_fraudulent_transactions_7d),
                ),
                Sum(
                    window=Continuous("1w"),
                    of="transaction_amount",
                    into_field=str(cls.sum_categ_fraudulent_transactions_7d),
                ),
            ]
        )


class TestFraudReportAggregatedDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_fraud(self, client):
        # # Sync the dataset
        client.commit(
            message="msg",
            datasets=[MerchantInfo, Activity, FraudReportAggregatedDataset],
        )
        now = datetime.now(timezone.utc)
        minute_ago = now - timedelta(minutes=1)
        data = [
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322}',
                minute_ago,
            ],
            [
                13423,
                "atm_withdrawal",
                99,
                '{"location": "mumbai"}',
                minute_ago,
            ],
            [
                14325,
                "report",
                99,
                '{"transaction_amount": 99, "merchant_id": 1422}',
                minute_ago,
            ],
            [
                18347,
                "atm_withdrawal",
                209,
                '{"location": "delhi"}',
                minute_ago,
            ],
            [
                18232,
                "report",
                49,
                '{"transaction_amount": 49, "merchant_id": 1322}',
                minute_ago,
            ],
            [
                18232,
                "report",
                149,
                '{"transaction_amount": 149, "merchant_id": 1422}',
                minute_ago,
            ],
            [
                18232,
                "report",
                999,
                '{"transaction_amount": 999, "merchant_id": 1322}',
                minute_ago,
            ],
            [
                18232,
                "report",
                199,
                '{"transaction_amount": 199, "merchant_id": 1322}',
                minute_ago,
            ],
        ]
        columns = ["user_id", "action_type", "amount", "metadata", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "Activity", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        two_days_ago = now - timedelta(days=2)
        data = [
            [1322, "grocery", "mumbai", two_days_ago],
            [1422, "entertainment", "delhi", two_days_ago],
        ]

        columns = ["merchant_id", "category", "location", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MerchantInfo", df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        now = datetime.now(timezone.utc) + timedelta(minutes=1)
        ts = pd.Series([now, now])
        categories = pd.Series(["grocery", "entertainment"])
        df, _ = FraudReportAggregatedDataset.lookup(ts, category=categories)

        assert df.shape == (2, 5)
        assert df["category"].tolist() == ["grocery", "entertainment"]
        assert df["num_categ_fraudulent_transactions"].tolist() == [4, 2]
        assert df["num_categ_fraudulent_transactions_7d"].tolist() == [4, 2]
        assert df["sum_categ_fraudulent_transactions_7d"].tolist() == [
            1296,
            248,
        ]

        if client.is_integration_client():
            return

        df = client.get_dataset_df("FraudReportAggregatedDataset")
        assert df.shape == (4, 5)
        assert df["category"].tolist() == [
            "entertainment",
            "grocery",
            "entertainment",
            "grocery",
        ]
        assert df["num_categ_fraudulent_transactions"].tolist() == [2, 4, 2, 4]
        assert df["num_categ_fraudulent_transactions_7d"].tolist() == [
            2,
            4,
            0,
            0,
        ]
        assert df["sum_categ_fraudulent_transactions_7d"].tolist() == [
            248,
            1296,
            0,
            0,
        ]


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("UserAge"), disorder="14d", cdc="append")
@dataset
class UserAge:
    name: str
    age: int
    city: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@source(webhook.endpoint("UserAgeNonTable"), disorder="14d", cdc="append")
@dataset
class UserAge2:
    name: str
    age: int
    city: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset(index=True)
class UserAgeAggregated:
    city: str = field(key=True)
    timestamp: datetime
    sum_age: int

    @pipeline
    @inputs(UserAge, UserAge2)
    def create_user_age_aggregated(cls, user_age: Dataset, user_age2: Dataset):
        union = user_age + user_age2
        return union.groupby("city").aggregate(
            [
                Sum(
                    window=Continuous("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )


class TestAggregateTableDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_table_aggregation(self, client):
        client.commit(
            message="msg", datasets=[UserAge, UserAge2, UserAgeAggregated]
        )
        client.sleep()
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        now = datetime.now(timezone.utc)
        tomorrow = datetime.now(timezone.utc) + timedelta(days=1)

        data = [
            ["Sonu", 24, "mumbai", yesterday],
            ["Monu", 25, "delhi", yesterday],
        ]

        columns = ["name", "age", "city", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserAge", input_df)
        assert response.status_code == requests.codes.OK, response.json()

        client.sleep()

        ts = pd.Series([now, now])
        names = pd.Series(["mumbai", "delhi"])
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]

        input_df["timestamp"] = tomorrow
        response = client.log("fennel_webhook", "UserAgeNonTable", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        df, _ = UserAgeAggregated.lookup(ts, city=names)
        assert df.shape == (2, 3)
        assert df["city"].tolist() == ["mumbai", "delhi"]
        assert df["sum_age"].tolist() == [24, 25]


################################################################################
#                           Dataset & Pipelines Complex E2E Tests
################################################################################


@meta(owner="gianni@fifa.com")
@source(webhook.endpoint("PlayerInfo"), disorder="14d", cdc="append")
@dataset
class PlayerInfo:
    name: str
    age: int
    height: int = field().meta(description="in inches")  # type: ignore
    weight: int = field().meta(description="in pounds")  # type: ignore
    club: str
    timestamp: datetime


@meta(owner="gianni@fifa.com")
@source(webhook.endpoint("ClubSalary"), disorder="14d", cdc="upsert")
@dataset(index=True)
class ClubSalary:
    club: str = field(key=True)
    timestamp: datetime
    salary: int


@meta(owner="gianni@fifa.com")
@source(webhook.endpoint("WAG"), disorder="14d", cdc="upsert")
@dataset(index=True)
class WAG:
    name: str = field(key=True)
    timestamp: datetime
    wag: str


@meta(owner="gianni@fifa.com")
@dataset(index=True)
class ManchesterUnitedPlayerInfo:
    name: str = field(key=True)
    timestamp: datetime
    age: int
    height: float = field().meta(description="in cm")  # type: ignore
    weight: float = field().meta(description="in kg")  # type: ignore
    club: str
    salary: Optional[int]
    wag: Optional[str]

    @pipeline
    @inputs(PlayerInfo, ClubSalary, WAG)
    def create_player_detailed_info(
        cls, player_info: Dataset, club_salary: Dataset, wag: Dataset
    ):
        def convert_to_metric_stats(df: pd.DataFrame) -> pd.DataFrame:
            df["height"] = df["height"] * 2.54
            df["weight"] = df["weight"] * 0.453592
            return df

        metric_stats = player_info.transform(
            convert_to_metric_stats,
            schema={
                "name": str,
                "age": int,
                "height": float,
                "weight": float,
                "club": str,
                "timestamp": datetime,
            },
        )
        ms = metric_stats.join(club_salary, how="left", on=["club"])
        man_players = ms.filter(lambda df: df["club"] == "Manchester United")
        ret = man_players.join(wag, how="left", on=["name"])
        return ret.groupby("name").first()


@meta(owner="gianni@fifa.com")
@dataset(index=True)
class ManchesterUnitedPlayerInfoBounded:
    name: str = field(key=True)
    timestamp: datetime
    age: int
    height: float = field().meta(description="in cm")  # type: ignore
    weight: float = field().meta(description="in kg")  # type: ignore
    club: str
    salary: Optional[int]
    wag: Optional[str]

    @pipeline
    @inputs(PlayerInfo, ClubSalary, WAG)
    def create_player_detailed_info(
        cls, player_info: Dataset, club_salary: Dataset, wag: Dataset
    ):
        def convert_to_metric_stats(df: pd.DataFrame) -> pd.DataFrame:
            df["height"] = df["height"] * 2.54
            df["weight"] = df["weight"] * 0.453592
            return df

        metric_stats = player_info.transform(
            convert_to_metric_stats,
            schema={
                "name": str,
                "age": int,
                "height": float,
                "weight": float,
                "club": str,
                "timestamp": datetime,
            },
        )
        player_info_with_salary = metric_stats.join(
            club_salary, how="left", on=["club"], within=("60s", "0s")
        )
        manchester_players = player_info_with_salary.filter(
            lambda df: df["club"] == "Manchester United"
        )
        return (
            manchester_players.join(
                wag, how="left", on=["name"], within=("forever", "60s")
            )
            .groupby("name")
            .first()
        )


class TestE2eIntegrationTestMUInfo(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_muinfo_e2e_test(self, client):
        client.commit(
            message="msg",
            datasets=[PlayerInfo, ClubSalary, WAG, ManchesterUnitedPlayerInfo],
        )
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        minute_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
        data = [
            ["Rashford", 25, 71, 154, "Manchester United", minute_ago],
            ["Maguire", 29, 76, 198, "Manchester United", minute_ago],
            ["Messi", 35, 67, 148, "PSG", minute_ago],
            ["Christiano Ronaldo", 37, 74, 187, "Al-Nassr", minute_ago],
            ["Christiano Ronaldo", 30, 74, 177, "Manchester United", yesterday],
            ["Antony", 22, 69, 139, "Manchester United", minute_ago],
        ]
        columns = ["name", "age", "height", "weight", "club", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "PlayerInfo", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            ["Manchester United", yesterday, 1000000],
            ["PSG", yesterday, 2000000],
            ["Al-Nassr", yesterday, 3000000],
        ]
        columns = ["club", "timestamp", "salary"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "ClubSalary", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            ["Rashford", yesterday, "Lucia"],
            ["Maguire", yesterday, "Fern"],
            ["Messi", yesterday, "Antonela"],
            ["Christiano Ronaldo", yesterday, "Georgina"],
            ["Antony", yesterday, "Rosilene"],
        ]
        columns = ["name", "timestamp", "wag"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "WAG", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        # Do a lookup
        # Check with Nikhil on timestamp for CR7 - yesterday
        names = pd.DataFrame(
            {
                "name": [
                    "Rashford",
                    "Maguire",
                    "Messi",
                    "Christiano Ronaldo",
                    "Antony",
                ]
            }
        )
        client.sleep()
        df, _ = client.lookup("ManchesterUnitedPlayerInfo", keys=names)
        assert df.shape == (5, 8)
        assert df["club"].tolist() == [
            "Manchester United",
            "Manchester United",
            pd.NA,
            "Manchester United",
            "Manchester United",
        ]
        assert df["salary"].tolist()[0] == 1000000
        assert df["salary"].tolist()[1] == 1000000
        assert pd.isna(df["salary"].tolist()[2])
        assert df["salary"].tolist()[3] == 1000000
        assert df["salary"].tolist()[4] == 1000000
        assert df["wag"].tolist() == [
            "Lucia",
            "Fern",
            pd.NA,
            "Georgina",
            "Rosilene",
        ]


class TestE2eIntegrationTestMUInfoBounded(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_muinfo_e2e_bounded(self, client):
        client.commit(
            message="msg",
            datasets=[
                PlayerInfo,
                ClubSalary,
                WAG,
                ManchesterUnitedPlayerInfoBounded,
            ],
        )

        now = datetime.now(timezone.utc)
        minute_ago = now - timedelta(minutes=1)
        second_ahead = now + timedelta(seconds=1)
        minute_ahead = now + timedelta(minutes=1)
        couple_minutes_ahead = now + timedelta(minutes=2)
        data = [
            ["Rashford", 25, 71, 154, "Manchester United", now],
            ["Maguire", 29, 76, 198, "Manchester United", now],
            ["Messi", 35, 67, 148, "PSG", now],
            ["Christiano Ronaldo", 37, 74, 187, "Al-Nassr", now],
            ["Christiano Ronaldo", 30, 74, 177, "Manchester United", now],
            [
                "Antony",
                22,
                69,
                139,
                "Manchester United",
                second_ahead,
            ],  # This will be skipped in salary join
        ]
        columns = ["name", "age", "height", "weight", "club", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "PlayerInfo", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            ["Manchester United", minute_ago, 1000000],
            ["PSG", minute_ago, 2000000],
            ["Al-Nassr", minute_ago, 3000000],
        ]
        columns = ["club", "timestamp", "salary"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "ClubSalary", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        data = [
            [
                "Rashford",
                couple_minutes_ahead,
                "Lucia",
            ],  # This will be skipped in wag join
            ["Maguire", minute_ahead, "Fern"],
            ["Messi", minute_ahead, "Antonela"],
            ["Christiano Ronaldo", minute_ahead, "Georgina"],
            ["Antony", minute_ahead, "Rosilene"],
        ]
        columns = ["name", "timestamp", "wag"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "WAG", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        ts = pd.Series(
            [
                couple_minutes_ahead,
                couple_minutes_ahead,
                couple_minutes_ahead,
                couple_minutes_ahead,
                couple_minutes_ahead,
            ]
        )
        if client.is_integration_client():
            time.sleep(30)
        df, _ = client.lookup(
            ManchesterUnitedPlayerInfoBounded,
            keys=pd.DataFrame(
                {
                    "name": [
                        "Rashford",
                        "Maguire",
                        "Messi",
                        "Christiano Ronaldo",
                        "Antony",
                    ]
                }
            ),
            timestamps=ts,
        )
        assert df.shape == (5, 8)
        assert df["club"].tolist() == [
            "Manchester United",
            "Manchester United",
            pd.NA,
            "Manchester United",
            "Manchester United",
        ]
        assert df["salary"].tolist() == [
            1000000,
            1000000,
            pd.NA,
            1000000,
            pd.NA,
        ]


@mock
def test_join(client):
    def test_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        assert df.shape == (3, 5), "Shape is not correct {}".format(df.shape)
        assert (
            "b1" not in df.columns
        ), "b1 column should not be present, " "{}".format(df.columns)
        return df

    @source(webhook.endpoint("A"), disorder="14d", cdc="upsert")
    @meta(owner="aditya@fennel.ai")
    @dataset
    class A:
        a1: int = field(key=True)
        v: int
        t: datetime

    @source(webhook.endpoint("B"), disorder="14d", cdc="upsert")
    @meta(owner="aditya@fennel.ai")
    @dataset(index=True)
    class B:
        b1: int = field(key=True)
        v2: int
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDataset:
        a1: int = field(key=True)
        v: int
        v2: Optional[int]
        t: datetime

        @pipeline
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            x = a.join(
                b,
                how="left",
                left_on=["a1"],
                right_on=["b1"],
            )  # type: ignore
            assert len(x.schema()) == 4
            x = x.transform(test_dataframe)  # type: ignore
            return x

    client.commit(message="msg", datasets=[A, B, ABCDataset])

    now = datetime.now(timezone.utc)
    df1 = pd.DataFrame(
        {
            "a1": [1, 2, 3],
            "v": [1, 2, 3],
            "t": [now, now, now],
        }
    )
    df2 = pd.DataFrame(
        {
            "b1": [1, 2, 4],
            "v2": [1, 2, 4],
            "t": [now, now, now],
        }
    )
    client.log("fennel_webhook", "A", df1)
    client.log("fennel_webhook", "B", df2)


def extract_payload(
    df: pd.DataFrame,
    payload_col: str = "payload",
    json_col: str = "json_payload",
) -> pd.DataFrame:
    df[json_col] = df[payload_col].apply(lambda x: json.loads(x))
    return df[["timestamp", json_col]]


def extract_keys(
    df: pd.DataFrame, json_col: str = "json_payload", keys: List[str] = []
) -> pd.DataFrame:
    for key in keys:
        df[key] = df[json_col].apply(lambda x: x[key])
        if key == "latitude" or key == "longitude":
            df[key] = df[key].astype(float)
    return df.drop(json_col, axis=1)


def extract_location_index(
    df: pd.DataFrame,
    index_col: str,
    latitude_col: str = "latitude",
    longitude_col: str = "longitude",
    resolution: int = 2,
) -> pd.DataFrame:
    df[index_col] = df.apply(
        lambda x: str(x[latitude_col])[0 : 3 + resolution]  # noqa
        + "-"  # noqa
        + str(x[longitude_col])[0 : 3 + resolution],  # noqa
        axis=1,
    )
    df[index_col] = df[index_col].astype(str)
    return df


@dataset
@source(webhook.endpoint("CommonEvent"), disorder="14d", cdc="append")
@meta(owner="aditya@fennel.ai")
class CommonEvent:
    name: str
    timestamp: datetime = field(timestamp=True)
    payload: str


@dataset(index=True)
@meta(owner="aditya@fennel.ai.com", description="Location index features")
class LocationLatLong:
    latlng2: str = field(key=True)
    timestamp: datetime = field(timestamp=True)
    onb_velocity_l1_2: int
    onb_velocity_l7_2: int
    onb_velocity_l30_2: int
    onb_velocity_l90_2: int
    onb_total_2: int

    @pipeline
    @includes(extract_payload, extract_keys, extract_location_index)
    @inputs(CommonEvent)
    def get_payload(cls, common_event: Dataset):
        event = common_event.filter(lambda x: x["name"] == "LocationLatLong")
        new_schema = {
            "timestamp": datetime,
            "latitude": float,
            "longitude": float,
        }
        data = (
            event.transform(
                extract_payload,
                schema={
                    "json_payload": Dict[str, float],
                    "timestamp": datetime,
                },
            )
            .assign(
                "latitude",
                float,
                lambda df: df["json_payload"].apply(lambda x: x["latitude"]),
            )
            .assign(
                "longitude",
                float,
                lambda df: df["json_payload"].apply(lambda x: x["longitude"]),
            )
            .drop("json_payload")
        )

        new_schema.update({"latlng2": str})
        data = data.transform(
            lambda x: extract_location_index(
                x, index_col="latlng2", resolution=2
            ),
            schema=new_schema,
        )

        return data.groupby("latlng2").aggregate(
            [
                Count(window=Continuous("1d"), into_field="onb_velocity_l1_2"),
                Count(window=Continuous("7d"), into_field="onb_velocity_l7_2"),
                Count(
                    window=Continuous("30d"), into_field="onb_velocity_l30_2"
                ),
                Count(
                    window=Continuous("90d"), into_field="onb_velocity_l90_2"
                ),
                Count(window=Continuous("forever"), into_field="onb_total_2"),
            ]
        )


def del_spaces_tabs_and_newlines(s):
    pattern = re.compile(r"^@meta.*$", re.MULTILINE)
    text = re.sub(pattern, "", s)
    return re.sub(r"[\s\n\t]+", "", text)


@pytest.mark.integration
@mock
def test_complex_lambda(client):
    # Test for code generation
    view = InternalTestClient()
    view.add(CommonEvent)
    view.add(LocationLatLong)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 2
    assert len(sync_request.operators) == 8

    expected_code = """
def extract_location_index(
        df: pd.DataFrame,
        index_col: str,
        latitude_col: str = "latitude",
        longitude_col: str = "longitude",
        resolution: int = 2,
) -> pd.DataFrame:
    df[index_col] = df.apply(
        lambda x: str(x[latitude_col])[0: 3 + resolution]  # noqa
                  + "-"  # noqa
                  + str(x[longitude_col])[0: 3 + resolution],  # noqa
        axis=1,
    )
    df[index_col] = df[index_col].astype(str)
    return df

def extract_keys(
        df: pd.DataFrame, json_col: str = "json_payload", keys: List[str] = []
) -> pd.DataFrame:
    for key in keys:
        df[key] = df[json_col].apply(lambda x: x[key])
        if key == "latitude" or key == "longitude":
            df[key] = df[key].astype(float)
    return df.drop(json_col, axis=1)


def extract_payload(
        df: pd.DataFrame,
        payload_col: str = "payload",
        json_col: str = "json_payload",
) -> pd.DataFrame:
    df[json_col] = df[payload_col].apply(lambda x: json.loads(x))
    return df[["timestamp", json_col]]

@dataset(index=True)
@meta(owner="aditya@fennel.ai.com", description="Location index features")
class LocationLatLong:
    latlng2: str = field(key=True)
    timestamp: datetime = field(timestamp=True)
    onb_velocity_l1_2: int
    onb_velocity_l7_2: int
    onb_velocity_l30_2: int
    onb_velocity_l90_2: int
    onb_total_2: int

    @classmethod
    def wrapper_adace968e2(cls, *args, **kwargs):

        def extract_payload(
                df: pd.DataFrame,
                payload_col: str = "payload",
                json_col: str = "json_payload",
        ) -> pd.DataFrame:
            df[json_col] = df[payload_col].apply(lambda x: json.loads(x))
            return df[["timestamp", json_col]]


        return extract_payload(*args, **kwargs)


def LocationLatLong_wrapper_adace968e2(*args, **kwargs):
    _fennel_internal = LocationLatLong.__fennel_original_cls__
    return getattr(_fennel_internal, "wrapper_adace968e2")(*args, **kwargs)

"""

    assert del_spaces_tabs_and_newlines(
        sync_request.operators[2].transform.pycode.generated_code
    ) == del_spaces_tabs_and_newlines(expected_code)

    # Test running of code
    client.commit(message="msg", datasets=[CommonEvent, LocationLatLong])

    data = [
        {
            "name": "LocationLatLong",
            "timestamp": datetime.now(timezone.utc),
            "payload": '{"user_id": 247, "latitude": 12.3, "longitude": 12.3}',
        },
        {
            "name": "LocationLatLong",
            "timestamp": datetime.now(timezone.utc),
            "payload": '{"user_id": 248, "latitude": 12.3, "longitude": 12.4}',
        },
        {
            "name": "LocationLatLong",
            "timestamp": datetime.now(timezone.utc),
            "payload": '{"user_id": 246, "latitude": 12.3, "longitude": 12.3}',
        },
    ]
    res = client.log("fennel_webhook", "CommonEvent", pd.DataFrame(data))
    assert res.status_code == 200, res.json()

    client.sleep()

    now = datetime.now(timezone.utc)
    timestamps = pd.Series([now, now])
    res, found = LocationLatLong.lookup(
        ts=timestamps, latlng2=pd.Series(["12.3-12.4", "12.3-12.3"])
    )
    assert found.tolist() == [True, True]
    assert res.shape == (2, 7)
    assert res["onb_velocity_l1_2"].tolist() == [1, 2]


@dataset
@source(
    Webhook(name="fennel_webhook").endpoint("common"),
    disorder="14d",
    cdc="append",
)
@meta(owner="nitin@epifi.com", tags=["common"])
class TransactionsCredit:
    updated_at: datetime = field(timestamp=True)
    to_account_id: str
    to_account_type: str
    payment_protocol: str
    provenance: str
    computed_amount: float


@dataset
@meta(owner="nitin@epifi.com", tags=["common"])
class TransactionsCreditInternetBanking:
    account_id: str = field(key=True)
    updated_at: datetime = field(timestamp=True)
    sum_credit_internet_banking: float
    min_credit_internet_banking: float
    max_credit_internet_banking: float
    mean_credit_internet_banking: float
    number_credit_internet_banking: int
    stddev_credit_internet_banking: float

    @pipeline
    @inputs(TransactionsCredit)
    def get_payload(cls, event: Dataset):
        event = (
            event.rename({"to_account_id": "account_id"})  # type: ignore
            .filter(lambda x: x["provenance"] == "EXTERNAL")
            .filter(lambda x: x["payment_protocol"].isin(["RTGS", "IMPS"]))
            .groupby("account_id")
            .aggregate(
                [
                    Sum(
                        of="computed_amount",
                        window=Continuous("forever"),
                        into_field="sum_credit_internet_banking",
                    ),
                    Min(
                        of="computed_amount",
                        window=Continuous("forever"),
                        into_field="min_credit_internet_banking",
                        default=float("inf"),
                    ),
                    Max(
                        of="computed_amount",
                        window=Continuous("forever"),
                        into_field="max_credit_internet_banking",
                        default=float("-inf"),
                    ),
                    Average(
                        of="computed_amount",
                        window=Continuous("forever"),
                        into_field="mean_credit_internet_banking",
                    ),
                    Count(
                        window=Continuous("forever"),
                        into_field="number_credit_internet_banking",
                    ),
                    Stddev(
                        of="computed_amount",
                        window=Continuous("forever"),
                        into_field="stddev_credit_internet_banking",
                    ),
                ]
            )
        )
        return event


@mock
def test_chained_lambda(client):
    client.commit(
        message="msg",
        datasets=[TransactionsCredit, TransactionsCreditInternetBanking],
    )
    view = InternalTestClient()
    view.add(TransactionsCredit)
    view.add(TransactionsCreditInternetBanking)
    sync_request = view._get_sync_request_proto()

    assert len(sync_request.pipelines) == 1
    assert len(sync_request.operators) == 5

    expected_code = """
@dataset
@meta(owner="nitin@epifi.com", tags=["common"])
class TransactionsCreditInternetBanking:
    account_id: str = field(key=True)
    updated_at: datetime = field(timestamp=True)
    sum_credit_internet_banking: float
    min_credit_internet_banking: float
    max_credit_internet_banking: float
    mean_credit_internet_banking: float
    number_credit_internet_banking: int
    stddev_credit_internet_banking: float

    @classmethod
    def wrapper_4d45b34b11(cls, *args, **kwargs):
        _fennel_internal = lambda x: x["payment_protocol"].isin(["RTGS", "IMPS"])
        return _fennel_internal(*args, **kwargs)


def TransactionsCreditInternetBanking_wrapper_4d45b34b11(*args, **kwargs):
    _fennel_internal = TransactionsCreditInternetBanking.__fennel_original_cls__
    return getattr(_fennel_internal, "wrapper_4d45b34b11")(*args, **kwargs)

def TransactionsCreditInternetBanking_wrapper_4d45b34b11_filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[TransactionsCreditInternetBanking_wrapper_4d45b34b11(df)]
    """

    assert del_spaces_tabs_and_newlines(
        sync_request.operators[3].filter.pycode.generated_code
    ) == del_spaces_tabs_and_newlines(expected_code)


@mock
def test_inner_join_column_name_collision(client):
    webhook = Webhook(name="fennel_webhook")

    @dataset
    @source(
        webhook.endpoint("PaymentEventDataset"),
        disorder="14d",
        cdc="upsert",
        env="local",
    )
    class PaymentEventDataset:
        customer: int = field(key=True)
        created: datetime
        outcome_risk_score: float

    @dataset(index=True)
    @source(
        webhook.endpoint("PaymentAccountDataset"),
        disorder="14d",
        cdc="upsert",
        env="local",
    )
    class PaymentAccountDataset:
        id: int
        created: datetime
        customer_id: int = field(key=True)

    @dataset(index=True)
    @source(
        webhook.endpoint("PaymentAccountAssociationDataset"),
        disorder="14d",
        cdc="upsert",
        env="local",
    )
    class PaymentAccountAssociationDataset:
        id: int = field(key=True)
        created: datetime
        account_id: int

    @dataset(index=True)
    @source(
        webhook.endpoint("AccountDataset"),
        disorder="14d",
        cdc="upsert",
        env="local",
    )
    class AccountDataset:
        id: int = field(key=True)
        created: datetime
        primary_rider_id: int

    @dataset
    class RiderAggRiskScore:
        primary_rider_id: int = field(key=True)
        created: datetime
        max_risk_score: float
        min_risk_score: float

        @pipeline
        @inputs(
            PaymentEventDataset,
            PaymentAccountDataset,
            PaymentAccountAssociationDataset,
            AccountDataset,
        )
        def stripe_enrichment(
            cls,
            stripe_charge: Dataset,
            payment_account: Dataset,
            payment_account_association: Dataset,
            account: Dataset,
        ):
            return (
                stripe_charge.join(
                    payment_account,
                    how="inner",
                    left_on=["customer"],
                    right_on=["customer_id"],
                )
                .join(payment_account_association, how="inner", on=["id"])
                .join(
                    account,
                    left_on=["account_id"],
                    right_on=["id"],
                    how="inner",
                )
                .groupby("primary_rider_id")
                .aggregate(
                    Max(
                        of="outcome_risk_score",
                        window=Continuous("forever"),
                        into_field="max_risk_score",
                        default=-1.0,
                    ),
                    Min(
                        of="outcome_risk_score",
                        window=Continuous("forever"),
                        into_field="min_risk_score",
                        default=-1.0,
                    ),
                )
            )

    initial = client.commit(
        message="msg",
        datasets=[
            RiderAggRiskScore,
            PaymentEventDataset,
            PaymentAccountDataset,
            PaymentAccountAssociationDataset,
            AccountDataset,
        ],
    )
    assert initial.status_code == 200
    now = datetime.now(timezone.utc)

    stripe_charge_df = pd.DataFrame(
        {"customer": [1], "created": [now], "outcome_risk_score": [0.5]}
    )
    stripe_charge_response = client.log(
        webhook="fennel_webhook",
        endpoint="PaymentEventDataset",
        df=stripe_charge_df,
    )
    assert stripe_charge_response.status_code == 200

    payment_account_df = pd.DataFrame(
        {"id": [1], "created": [now], "customer_id": [1]}
    )
    payment_account_response = client.log(
        webhook="fennel_webhook",
        endpoint="PaymentAccountDataset",
        df=payment_account_df,
    )
    assert payment_account_response.status_code == 200

    payment_account_association_df = pd.DataFrame(
        {"id": [1], "created": [now], "account_id": [1]}
    )
    payment_account_association_response = client.log(
        webhook="fennel_webhook",
        endpoint="PaymentAccountAssociationDataset",
        df=payment_account_association_df,
    )
    assert payment_account_association_response.status_code == 200

    account_df = pd.DataFrame(
        {"id": [1], "created": [now], "primary_rider_id": [1]}
    )
    account_response = client.log(
        webhook="fennel_webhook", endpoint="AccountDataset", df=account_df
    )

    assert account_response.status_code == 200

    extracted_df = client.get_dataset_df("RiderAggRiskScore")
    assert extracted_df.shape[0] == 1
    assert extracted_df["max_risk_score"].iloc[0] == 0.5


@meta(owner="test@test.com")
@source(
    webhook.endpoint("UserInfoDatasetPreProc"),
    disorder="14d",
    cdc="upsert",
    preproc={
        "age": 10,
        "country": ref("upstream.country"),
        "timestamp": datetime(1970, 1, 1, 0, 0, 0),
    },
)
@dataset(index=True)
class UserInfoDatasetPreProc:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


@mock
def test_dataset_with_pre_proc_log(client):
    # Sync the dataset
    client.commit(message="msg", datasets=[UserInfoDatasetPreProc])
    now = datetime.now(timezone.utc)
    data = [
        [18232, "Ross", "USA"],
        [18234, "Monica", "Chile"],
    ]
    columns = ["user_id", "name", "upstream.country"]
    df = pd.DataFrame(data, columns=columns)
    response = client.log("fennel_webhook", "UserInfoDatasetPreProc", df)
    assert response.status_code == requests.codes.OK, response.json()

    # Do lookup on UserInfoDataset
    if client.is_integration_client():
        client.sleep()
    ts = pd.Series([now, now])
    user_id_keys = pd.Series([18232, 18234])
    df, found = UserInfoDatasetPreProc.lookup(ts, user_id=user_id_keys)
    assert df.shape == (2, 5)
    assert df["user_id"].tolist() == [18232, 18234]
    assert df["name"].tolist() == ["Ross", "Monica"]
    assert df["age"].tolist() == [
        10,
        10,
    ]  # should return the default value of 10 assigned above
    assert df["country"].tolist() == ["USA", "Chile"]
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert df["timestamp"].tolist() == [epoch, epoch]


@pytest.mark.integration
@mock
def test_lookup_as_of_now(client):
    client.commit(message="msg", datasets=[UserInfoDataset])
    data = [
        [18232, "Ross", 24, "USA", 1668475993],
        [18234, "Monica", 24, "USA", 1668475343],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    client.sleep()

    data, found = client.lookup(
        "UserInfoDataset",
        pd.DataFrame({"user_id": [18232]}),
        fields=["name"],
    )
    assert found.tolist() == [True]
    assert len(data) == 1
    assert data.tolist() == ["Ross"]


@pytest.mark.integration
@mock
def test_lookup_as_of_time(client):
    client.commit(message="msg", datasets=[UserInfoDataset])
    data = [
        [18232, "Ross", 24, "USA", "2022-11-10 01:22:23"],
        [18233, "Monica", 24, "USA", "2022-11-15 01:33:13"],
        [18234, "Chandler", 24, "USA", "2022-11-16 01:22:23"],
        [18235, "Rachel", 24, "USA", "2022-11-17 01:33:13"],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    client.sleep()

    data, found = client.lookup(
        UserInfoDataset,
        keys=pd.DataFrame({"user_id": [18232, 18233]}),
        fields=["name"],
        timestamps=pd.Series(["2020-11-09 01:22:23", "2022-11-16 01:33:13"]),
    )
    assert len(found.tolist()) == 2
    assert found.tolist() == [False, True]
    assert data.shape[0] == 2
    assert data.tolist() == [pd.NA, "Monica"]


@pytest.mark.integration
@mock
def test_erase_key(client):
    client.commit(
        message="msg", datasets=[UserInfoDataset, UserInfoDatasetDerived]
    )
    data = [
        [18232, "Ross", 24, "USA", "2022-11-10 01:22:23"],
        [18233, "Monica", 24, "USA", "2022-11-15 01:33:13"],
        [18234, "Chandler", 24, "USA", "2022-11-16 01:22:23"],
        [18235, "Rachel", 24, "USA", "2022-11-17 01:33:13"],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    client.sleep()

    data, found = client.lookup(
        "UserInfoDatasetDerived",
        pd.DataFrame({"user_id": [18232]}),
        fields=["name"],
    )
    assert len(found.tolist()) == 1
    assert found.tolist() == [True]
    assert len(data) == 1
    assert data[0] == "Ross"

    # Issue deletion

    response = client.erase(
        UserInfoDatasetDerived, pd.DataFrame({"user_id": [18232]})
    )
    assert response.status_code == requests.codes.OK, response.json()
    client.sleep()
    client.sleep()
    client.sleep()
    client.sleep()
    client.sleep()

    # Should be deleted
    data, found = client.lookup(
        "UserInfoDatasetDerived",
        pd.DataFrame({"user_id": [18232]}),
        fields=["name"],
    )
    assert len(found.tolist()) == 1
    assert found.tolist() == [False]

    # Should be deleted as of
    now = datetime.now(timezone.utc)
    data, found = client.lookup(
        "UserInfoDatasetDerived",
        pd.DataFrame({"user_id": [18232]}),
        fields=["name"],
        timestamps=pd.Series([now]),
    )
    assert len(found.tolist()) == 1
    assert found.tolist() == [False]

    # Even when logging again it should be filter out

    data = [
        [18232, "Ross", 24, "USA", "2022-11-10 01:22:23"],
        [18233, "Monica", 24, "USA", "2022-11-15 01:33:13"],
        [18234, "Chandler", 24, "USA", "2022-11-16 01:22:23"],
        [18235, "Rachel", 24, "USA", "2022-11-17 01:33:13"],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    client.sleep()
    client.sleep()
    client.sleep()

    data, found = client.lookup(
        "UserInfoDatasetDerived",
        pd.DataFrame({"user_id": [18232]}),
        fields=["name"],
    )
    assert len(found.tolist()) == 1
    assert found.tolist() == [False]

    data, found = client.lookup(
        "UserInfoDatasetDerived",
        pd.DataFrame({"user_id": [18232]}),
        fields=["name"],
        timestamps=pd.Series([now]),
    )
    assert len(found.tolist()) == 1
    assert found.tolist() == [False]


@dataset
class UserInfo:
    user_id: int
    city: Optional[str]
    ts: datetime


@dataset(index=True)
class LastCityVisted:
    user_id: int = field(key=True)
    city: Optional[str]
    ts: datetime

    @pipeline
    @inputs(UserInfo)
    def last_city_visited(cls, user_info: Dataset):
        return user_info.groupby("user_id").latest()


@dataset(index=True)
class CityCount:
    city: str = field(key=True)
    cnt: int
    ts: datetime

    @pipeline
    @inputs(LastCityVisted)
    def city_count(cls, last_city_visted: Dataset):
        return (
            last_city_visted.dropnull()
            .groupby("city")
            .aggregate(cnt=Count(window=Continuous("forever")))
        )


@mock
def test_latest_operator_query_offline(client):
    client.commit(
        datasets=[UserInfo, LastCityVisted, CityCount], message="test"
    )

    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 2, 2, 1],
            "city": ["A", "B", "C", None, "A", "B", "B"],
            "ts": [
                datetime(2022, 1, 2),
                datetime(2022, 1, 4),
                datetime(2022, 1, 6),
                datetime(2022, 1, 7),
                datetime(2022, 1, 8),
                datetime(2022, 1, 10),
                datetime(2022, 1, 13),
            ],
        }
    )
    log(UserInfo, df)
    results, found = client.lookup(
        "LastCityVisted",
        keys=pd.DataFrame({"user_id": [1, 1, 1, 1, 2, 2, 2]}),
        timestamps=pd.Series(
            [
                datetime(2022, 1, 1),
                datetime(2022, 1, 3),  # (1, A, 1), ( 1, B, 3)
                datetime(2022, 1, 4),
                datetime(2022, 1, 8),
                datetime(2022, 1, 7),
                datetime(2022, 1, 9),
                datetime(2022, 1, 12),
            ]
        ),
    )
    assert found.tolist() == [False, True, True, True, False, True, True]
    assert results.shape == (7, 3)
    assert results["city"].tolist() == [pd.NA, "A", "B", pd.NA, pd.NA, "A", "B"]

    results, found = client.lookup(
        "CityCount",
        keys=pd.DataFrame({"city": ["A", "A", "A", "A", "B", "B", "B", "B"]}),
        timestamps=pd.Series(
            [
                datetime(2022, 1, 1),
                datetime(2022, 1, 3),
                datetime(2022, 1, 4),
                datetime(2022, 1, 8),
                datetime(2022, 1, 7),
                datetime(2022, 1, 9),
                datetime(2022, 1, 12),
                datetime(2022, 1, 14),
            ]
        ),
    )
    assert found.tolist() == [False, True, True, True, True, True, True, True]
    assert results.shape == (8, 3)
    # First row is not present because its before any logging
    # Then user visits A at t=2
    # But then he moves to B, hence count of A is 0 at t=3, then A gets visited at t=8
    # B has a cnt =1 at t=3, but then user 1 goes to C and None so he doesnt contribute
    # Finally user contrinbutes only from t=10 onwards, later user 1 joins him
    assert results["cnt"].tolist() == [pd.NA, 1, 0, 1, 0, 0, 1, 2]


@mock
def test_optional_list_type(client):

    @dataset
    @source(webhook.endpoint("A"), disorder="14d", cdc="append")
    class A:
        user_id: int
        value: Optional[int]
        ts: datetime

    @dataset(index=True)
    class B:
        user_id: int = field(key=True)
        value: List[Optional[int]]
        ts: datetime

        @pipeline
        @inputs(A)
        def pipeline(cls, event: Dataset):
            return event.groupby("user_id").aggregate(
                value=LastK(
                    of="value",
                    window=Continuous("forever"),
                    dedup=True,
                    limit=1,
                )
            )

    client.commit(datasets=[A, B], message="test")

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "value": [None, 1, 2],
            "ts": [now, now, now],
        }
    )
    client.log("fennel_webhook", "A", df)

    results, _ = client.lookup(
        B,
        keys=pd.DataFrame({"user_id": [1, 2, 3]}),
    )
    assert results["value"].tolist() == [[pd.NA], [1], [2]]


@mock
def test_last_k_on_struct_type(client):

    @struct
    class Value:
        value: int
        ts2: datetime

    @includes(Value)
    def get_value_struct(points) -> Value:
        value, ts2 = points
        return Value(value=value, ts2=ts2)  # type: ignore

    def get_value_map(points) -> Dict[str, str]:
        value, ts2 = points
        return {"value": str(value), "ts2": str(ts2)}

    @dataset
    @source(webhook.endpoint("A"), disorder="14d", cdc="append")
    class A:
        user_id: int
        value: int
        ts1: datetime = field(timestamp=True)
        ts2: datetime

    @dataset(index=True)
    class B:
        user_id: int = field(key=True)
        values_struct: List[Value]
        values_map: List[Dict[str, str]]
        ts1: datetime = field(timestamp=True)

        @pipeline
        @includes(Value, get_value_struct, get_value_map)
        @inputs(A)
        def pipeline(cls, event: Dataset):
            return (
                event.assign(
                    "values_struct",
                    Value,
                    lambda x: x[["value", "ts2"]].apply(
                        get_value_struct, axis=1
                    ),
                )
                .assign(
                    "values_map",
                    Dict[str, str],
                    lambda x: x[["value", "ts2"]].apply(get_value_map, axis=1),
                )
                .groupby("user_id")
                .aggregate(
                    values_struct=LastK(
                        of="values_struct",
                        window=Continuous("forever"),
                        dedup=True,
                        limit=100,
                    ),
                    values_map=LastK(
                        of="values_map",
                        window=Continuous("forever"),
                        dedup=True,
                        limit=100,
                    ),
                )
            )

    if sys.version_info >= (3, 10):

        client.commit(datasets=[A, B], message="test")

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 1, 2, 2, 3],
                "value": [1, 2, 3, 4, 5],
                "ts1": [now, now, now, now, now],
                "ts2": [now, now, now, now, now],
            }
        )
        client.log("fennel_webhook", "A", df)

        results, _ = client.lookup(
            B,
            keys=pd.DataFrame({"user_id": [1, 2, 3]}),
        )
        assert [
            [y.as_json() for y in x] for x in results["values_struct"].tolist()
        ] == [
            [{"value": 2, "ts2": now}, {"value": 1, "ts2": now}],
            [{"value": 4, "ts2": now}, {"value": 3, "ts2": now}],
            [{"value": 5, "ts2": now}],
        ]
        assert results["values_map"].tolist() == [
            [{"value": "2", "ts2": str(now)}, {"value": "1", "ts2": str(now)}],
            [{"value": "4", "ts2": str(now)}, {"value": "3", "ts2": str(now)}],
            [{"value": "5", "ts2": str(now)}],
        ]


@mock
def test_unkey_operator(client):
    @dataset
    class JobOpening:
        creater_id: int
        job_id: int
        creation_ts: datetime

    @dataset(index=True)
    class JobRank:
        creater_id: int = field(key=True)
        rank: int = field(key=True)
        creation_ts: datetime

        @pipeline
        @inputs(JobOpening)
        def rank_job(cls, job_opening: Dataset):
            return (
                job_opening.groupby("creater_id")
                .aggregate(
                    rank=Count(window=Continuous("forever")), emit="final"
                )
                .changelog("del")
                .filter(lambda df: ~df["del"] & (df["rank"] < 4))
                .drop("del")
                .groupby("creater_id", "rank")
                .latest()
            )

    client.commit(datasets=[JobOpening, JobRank], message="test")
    # Creation ts is 1 every 2 hours
    creation_ts = [
        datetime(2022, 1, 1, 2, 0, 0),
        datetime(2022, 1, 1, 4, 0, 0),
        datetime(2022, 1, 1, 6, 0, 0),
        datetime(2022, 1, 1, 8, 0, 0),
        datetime(2022, 1, 1, 10, 0, 0),
        datetime(2022, 1, 1, 12, 0, 0),
    ]
    openings = pd.DataFrame(
        {
            "creater_id": [1, 2, 1, 1, 1, 1],
            "job_id": [1, 2, 3, 4, 5, 6],
            "creation_ts": creation_ts,
        }
    )
    log(JobOpening, openings)
    results, found = client.lookup(
        "JobRank",
        keys=pd.DataFrame(
            {"creater_id": [1, 1, 1, 1, 2], "rank": [4, 3, 2, 1, 1]}
        ),
    )
    assert found.tolist() == [False, True, True, True, True]
    assert results.shape == (5, 3)
    assert results["rank"].tolist() == [4, 3, 2, 1, 1]
    assert results["creation_ts"].tolist() == [
        pd.NaT,
        pd.Timestamp("2022-01-01 08:00:00", tz="UTC"),
        pd.Timestamp("2022-01-01 06:00:00", tz="UTC"),
        pd.Timestamp("2022-01-01 02:00:00", tz="UTC"),
        pd.Timestamp("2022-01-01 04:00:00", tz="UTC"),
    ]


webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("RawBase64EncodedData"), cdc="append", disorder="14d")
@dataset
class RawBase64EncodedData:
    payload: str
    ts: datetime


@dataset
class RawBytesData:
    payload_decoded: bytes
    ts: datetime

    @pipeline
    @inputs(RawBase64EncodedData)
    def pipeline(cls, event: Dataset):
        def t(df: pd.DataFrame) -> pd.DataFrame:
            import base64

            return df.assign(
                payload_decoded=df["payload"].apply(base64.b64decode)
            )

        schema = event.schema()
        schema["payload_decoded"] = bytes
        return event.transform(t, schema).drop("payload")


@dataset
class RawBytesDataDerived:
    payload_str: str
    ts: datetime

    @pipeline
    @inputs(RawBytesData)
    def base64_decode(cls, event: Dataset):
        def decode(df: pd.DataFrame) -> pd.DataFrame:
            import base64

            return df.assign(
                payload_str=df["payload_decoded"].apply(base64.b64encode)
            )

        schema = event.schema()
        schema["payload_str"] = str
        return event.transform(decode, schema).drop("payload_decoded")


@pytest.mark.integration
@mock
def test_bytes(client):
    client.commit(
        datasets=[RawBase64EncodedData, RawBytesData, RawBytesDataDerived],
        message="test",
    )

    df = pd.DataFrame(
        {
            "payload": ["aGVsbG8=", "d29ybGQ="],
            "ts": [datetime(2022, 1, 1), datetime(2022, 1, 2)],
        }
    )
    client.log("fennel_webhook", "RawBase64EncodedData", df)
    client.sleep()

    df_derived = client.inspect("RawBytesDataDerived")
    # Assert df and df_derived are same
    assert df_derived.shape == (2, 2)
    assert sorted(df_derived["payload_str"].tolist()) == [
        "aGVsbG8=",
        "d29ybGQ=",
    ]

    df_intermediate = client.inspect("RawBytesData")
    assert df_intermediate.shape == (2, 2)
    if client.is_integration_client():
        # Server base64 encodes the bytes
        assert sorted(df_intermediate["payload_decoded"].tolist()) == [
            "aGVsbG8=",
            "d29ybGQ=",
        ]
    else:
        assert df_intermediate["payload_decoded"].tolist() == [
            b"hello",
            b"world",
        ]


@mock
def test_log_to_dataset(client):
    client.commit(
        datasets=[RawBase64EncodedData, RawBytesData, RawBytesDataDerived],
        message="test",
    )

    df = pd.DataFrame(
        {
            "payload_decoded": [b"hello", b"world"],
            "ts": [datetime(2022, 1, 1), datetime(2022, 1, 2)],
        }
    )

    log(RawBytesData, df)

    df_derived = client.inspect("RawBytesDataDerived")
    # Assert df and df_derived are same
    assert df_derived.shape == (2, 2)
    assert sorted(df_derived["payload_str"].tolist()) == [
        "aGVsbG8=",
        "d29ybGQ=",
    ]


@mock
def test_empty_right_on_left_join(client):
    @dataset
    class A:
        a: int
        ts: datetime

    @dataset(index=True)
    class B:
        a: int = field(key=True)
        b: int
        c: int
        ts: datetime

    @dataset
    class C:
        a: int
        b: Optional[int]
        c: Optional[int]
        ts: datetime

        @pipeline
        @inputs(A, B)
        def pipeline(cls, a: Dataset, b: Dataset):
            return a.join(b, how="left", on=["a"])

    client.commit(datasets=[A, B, C], message="Test")
    log(
        A,
        pd.DataFrame(
            {"a": [1, 2], "ts": [datetime(2021, 1, 1), datetime(2021, 1, 2)]}
        ),
    )

    df = client.inspect("C")
    assert df.shape == (2, 4)
    assert df["b"].tolist() == [pd.NA, pd.NA]
    assert df["c"].tolist() == [pd.NA, pd.NA]
    assert df["a"].tolist() == [1, 2]
