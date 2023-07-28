import unittest
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from typing import Optional

import fennel._vendor.requests as requests
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
)
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import Embedding, inputs, outputs
from fennel.sources import source, Webhook
from fennel.test_lib import mock

################################################################################
#                           Feature Single Extractor Unit Tests
################################################################################

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str

    @expectations
    def get_expectations(cls):
        return [
            expect_column_values_to_be_between(
                column="age", min_value=1, max_value=100, mostly=0.95
            )
        ]


@meta(owner="test@test.com")
@featureset
class UserInfoSingleExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor(depends_on=[UserInfoDataset])
    @inputs(userid)
    @outputs(age, age_squared, age_cubed, is_name_common)
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df[str(cls.userid)] = user_id
        df[str(cls.age_squared)] = df["age"] ** 2
        df[str(cls.age_cubed)] = df["age"] ** 3
        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                str(cls.age),
                str(cls.age_squared),
                str(cls.age_cubed),
                str(cls.is_name_common),
            ]
        ]


def get_country_geoid(country: str) -> int:
    if country == "Russia":
        return 1
    elif country == "Chile":
        return 3
    else:
        return 5


@meta(owner="test@test.com")
@featureset
class UserInfoMultipleExtractor:
    userid: int = feature(id=1)
    name: str = feature(id=2)
    country_geoid: int = feature(id=3)
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)
    age_reciprocal: float = feature(id=8)

    @extractor(depends_on=[UserInfoDataset])
    @inputs(userid)
    @outputs(age, name)
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df[[str(cls.age), str(cls.name)]]

    @extractor
    @inputs(age, name)
    @outputs(age_squared, age_cubed, is_name_common)
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        is_name_common = name.isin(cls.get_common_names())
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            str(cls.age_squared),
            str(cls.age_cubed),
            str(cls.is_name_common),
        ]
        return df

    @extractor
    @inputs(age)
    @outputs(age_reciprocal)
    def get_age_reciprocal(cls, ts: pd.Series, age: pd.Series):
        d = age.apply(lambda x: 1 / (x / (3600.0 * 24)) + 0.01)
        return pd.Series(name="age_reciprocal", data=d)

    @extractor(depends_on=[UserInfoDataset], version=2)
    @includes(get_country_geoid)
    @inputs(userid)
    @outputs(country_geoid)
    def get_country_geoid_extractor(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["country_geoid"] = df["country"].apply(get_country_geoid)
        return df["country_geoid"]

    @classmethod
    def get_common_names(cls):
        return ["John", "Mary", "Bob"]


class TestSimpleExtractor(unittest.TestCase):
    @pytest.mark.integration
    def test_get_age_and_name_features(self):
        age = pd.Series([32, 24])
        name = pd.Series(["John", "Rahul"])
        ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 1)])
        df = UserInfoMultipleExtractor.get_age_and_name_features(
            UserInfoMultipleExtractor.original_cls, ts, age, name
        )
        self.assertEqual(df.shape, (2, 3))
        self.assertEqual(
            df["UserInfoMultipleExtractor.age_squared"].tolist(),
            [1024, 576],
        )

        self.assertEqual(
            df["UserInfoMultipleExtractor.age_cubed"].tolist(),
            [32768, 13824],
        )
        self.assertEqual(
            df["UserInfoMultipleExtractor.is_name_common"].tolist(),
            [True, False],
        )

    @pytest.mark.integration
    @mock
    def test_simple_extractor(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.now()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        ts = pd.Series([now, now])
        user_ids = pd.Series([18232, 18234])
        df = UserInfoSingleExtractor.get_user_info(
            UserInfoMultipleExtractor, ts, user_ids
        )
        self.assertEqual(df.shape, (2, 4))
        self.assertEqual(df["UserInfoSingleExtractor.age"].tolist(), [32, 24])
        self.assertEqual(
            df["UserInfoSingleExtractor.age_squared"].tolist(), [1024, 576]
        )
        self.assertEqual(
            df["UserInfoSingleExtractor.age_cubed"].tolist(), [32768, 13824]
        )
        self.assertEqual(
            df["UserInfoSingleExtractor.is_name_common"].tolist(), [True, False]
        )

        series = UserInfoMultipleExtractor.get_country_geoid_extractor(
            UserInfoMultipleExtractor, ts, user_ids
        )
        assert series.tolist() == [5, 3]


class TestExtractorDAGResolution(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_dag_resolution2(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.utcnow()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoMultipleExtractor.userid,
                UserInfoMultipleExtractor.name,
                UserInfoMultipleExtractor.country_geoid,
                UserInfoMultipleExtractor.age,
                UserInfoMultipleExtractor.age_squared,
                UserInfoMultipleExtractor.age_cubed,
                UserInfoMultipleExtractor.is_name_common,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))

        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoMultipleExtractor,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 8))
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_reciprocal"]),
            [2700.01, 3600.01],
        )


@meta(owner="test@test.com")
@featureset
class UserInfoTransformedFeatures:
    age_power_four: int = feature(id=1)
    is_name_common: bool = feature(id=2)
    country_geoid_square: int = feature(id=3)

    @extractor
    @inputs(
        UserInfoMultipleExtractor.age,
        UserInfoMultipleExtractor.is_name_common,
        UserInfoMultipleExtractor.country_geoid,
    )
    def get_user_transformed_features(
        cls,
        ts: pd.Series,
        user_age: pd.Series,
        is_name_common: pd.Series,
        country_geoid: pd.Series,
    ):
        age_power_four = user_age**4
        country_geoid = country_geoid**2
        return pd.DataFrame(
            {
                str(cls.age_power_four): age_power_four,
                str(cls.is_name_common): is_name_common,
                str(cls.country_geoid_square): country_geoid,
            }
        )


class TestExtractorDAGResolutionComplex(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_dag_resolution_complex(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[
                UserInfoMultipleExtractor,
                UserInfoTransformedFeatures,
            ],
        )
        client.sleep()
        now = datetime.utcnow()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoTransformedFeatures.age_power_four,
                UserInfoTransformedFeatures.is_name_common,
                UserInfoTransformedFeatures.country_geoid_square,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 3))
        # Write feature_df to std error so that it is visible in the test output
        self.assertEqual(
            feature_df["UserInfoTransformedFeatures.age_power_four"].tolist(),
            [1048576, 331776],
        )
        self.assertEqual(
            feature_df["UserInfoTransformedFeatures.is_name_common"].tolist(),
            [True, False],
        )
        self.assertEqual(
            feature_df[
                "UserInfoTransformedFeatures.country_geoid_square"
            ].tolist(),
            [25, 9],
        )

        if client.is_integration_client():
            return

        feature_df = client.extract_historical_features(
            output_feature_list=[
                UserInfoTransformedFeatures.age_power_four,
                UserInfoTransformedFeatures.is_name_common,
                UserInfoTransformedFeatures.country_geoid_square,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {
                    "UserInfoMultipleExtractor.userid": [18232, 18234],
                    "timestamps": [now, now],
                }
            ),
            timestamp_column="timestamps",
        )

        self.assertEqual(feature_df.shape, (2, 4))
        self.assertEqual(
            feature_df["UserInfoTransformedFeatures.age_power_four"].tolist(),
            [1048576, 331776],
        )
        self.assertEqual(
            feature_df["UserInfoTransformedFeatures.is_name_common"].tolist(),
            [True, False],
        )
        self.assertEqual(
            feature_df[
                "UserInfoTransformedFeatures.country_geoid_square"
            ].tolist(),
            [25, 9],
        )


# Embedding tests


@meta(owner="aditya@fennel.ai")
@source(webhook.endpoint("DocumentContentDataset"))
@dataset
class DocumentContentDataset:
    doc_id: int = field(key=True)
    bert_embedding: Embedding[4]
    fast_text_embedding: Embedding[3]
    num_words: int
    timestamp: datetime = field(timestamp=True)


@meta(owner="aditya@fennel.ai")
@featureset
class DocumentFeatures:
    doc_id: int = feature(id=1)
    bert_embedding: Embedding[4] = feature(id=2)
    fast_text_embedding: Embedding[3] = feature(id=3)
    num_words: int = feature(id=4)

    @extractor(depends_on=[DocumentContentDataset])
    @inputs(doc_id)
    @outputs(num_words, bert_embedding, fast_text_embedding)
    def get_doc_features(cls, ts: pd.Series, doc_id: pd.Series):
        df, _ = DocumentContentDataset.lookup(ts, doc_id=doc_id)  # type: ignore

        df[str(cls.bert_embedding)] = df[str(cls.bert_embedding)].apply(
            lambda x: x if x is not None else [0, 0, 0, 0]
        )
        df[str(cls.fast_text_embedding)] = df[
            str(cls.fast_text_embedding)
        ].apply(lambda x: x if x is not None else [0, 0, 0])
        df[str(cls.num_words)].fillna(0, inplace=True)
        return df[
            [
                str(cls.bert_embedding),
                str(cls.fast_text_embedding),
                str(cls.num_words),
            ]
        ]


class TestDocumentDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_document_featureset(self, client):
        client.sync(
            datasets=[DocumentContentDataset], featuresets=[DocumentFeatures]
        )
        now = datetime.utcnow()
        data = [
            [18232, np.array([1, 2, 3, 4]), np.array([1, 2, 3]), 10, now],
            [
                18234,
                np.array([1, 2.2, 0.213, 0.343]),
                np.array([0.87, 2, 3]),
                9,
                now,
            ],
            [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
        ]
        columns = [
            "doc_id",
            "bert_embedding",
            "fast_text_embedding",
            "num_words",
            "timestamp",
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "DocumentContentDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        feature_df = client.extract_features(
            output_feature_list=[
                DocumentFeatures,
            ],
            input_feature_list=[DocumentFeatures.doc_id],
            input_dataframe=pd.DataFrame(
                {"DocumentFeatures.doc_id": [18232, 18234]}
            ),
        )
        assert feature_df.shape == (2, 4)
        assert feature_df.columns.tolist() == [
            "DocumentFeatures.doc_id",
            "DocumentFeatures.bert_embedding",
            "DocumentFeatures.fast_text_embedding",
            "DocumentFeatures.num_words",
        ]
        assert feature_df["DocumentFeatures.doc_id"].tolist() == [
            18232,
            18234,
        ]
        assert feature_df["DocumentFeatures.num_words"].tolist() == [
            10,
            9,
        ]
        assert (
            feature_df["DocumentFeatures.bert_embedding"].tolist()[0]
            == [1, 2, 3, 4]
        ).all()

        yesterday = datetime.now() - timedelta(days=1)

        if client.is_integration_client():
            return

        feature_df = client.extract_historical_features(
            output_feature_list=[
                DocumentFeatures,
            ],
            input_feature_list=[DocumentFeatures.doc_id],
            input_dataframe=pd.DataFrame(
                {
                    "DocumentFeatures.doc_id": [18232, 18234],
                    "timestamps": [yesterday, yesterday],
                }
            ),
            timestamp_column="timestamps",
        )
        assert feature_df.shape == (2, 5)
        assert feature_df.columns.tolist() == [
            "DocumentFeatures.doc_id",
            "DocumentFeatures.bert_embedding",
            "DocumentFeatures.fast_text_embedding",
            "DocumentFeatures.num_words",
            "timestamps",
        ]
        assert feature_df["DocumentFeatures.doc_id"].tolist() == [
            18232,
            18234,
        ]
        assert feature_df["DocumentFeatures.num_words"].tolist() == [0, 0]
