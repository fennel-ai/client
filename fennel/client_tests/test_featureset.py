import unittest
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import pytest
import requests

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame, Embedding
from fennel.test_lib import mock_client


################################################################################
#                           Feature Single Extractor Unit Tests
################################################################################


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


@meta(owner="test@test.com")
@featureset
class UserInfoSingleExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @classmethod
    @extractor
    @depends_on(UserInfoDataset)
    def get_user_info(
            cls, ts: Series[datetime], user_id: Series[userid]
    ) -> DataFrame[age, age_squared, age_cubed, is_name_common]:
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["userid"] = user_id
        df["age_squared"] = df["age"] ** 2
        df["age_cubed"] = df["age"] ** 3
        df["is_name_common"] = df["name"].isin(["John", "Mary", "Bob"])
        return df[["age", "age_squared", "age_cubed", "is_name_common"]]


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
    country_geoid: int = feature(id=3).meta(wip=True)  # type: ignore
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @classmethod
    @extractor
    @depends_on(UserInfoDataset)
    def get_user_age_and_name(
            cls, ts: Series[datetime], user_id: Series[userid]
    ) -> DataFrame[age, name]:
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df[["age", "name"]]

    @classmethod
    @extractor
    def get_age_and_name_features(
            cls, ts: Series[datetime], user_age: Series[age], name: Series[name]
    ) -> DataFrame[age_squared, age_cubed, is_name_common]:
        is_name_common = name.isin(["John", "Mary", "Bob"])
        df = pd.concat([user_age ** 2, user_age ** 3, is_name_common], axis=1)
        df.columns = ["age_squared", "age_cubed", "is_name_common"]
        return df

    @classmethod
    @extractor
    @depends_on(UserInfoDataset)
    def get_country_geoid(
            cls, ts: Series[datetime], user_id: Series[userid]
    ) -> Series[country_geoid]:
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df["country"].apply(get_country_geoid)


class TestSimpleExtractor(unittest.TestCase):
    @pytest.mark.integration
    def test_get_age_and_name_features(self):
        age = pd.Series([32, 24])
        name = pd.Series(["John", "Rahul"])
        ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 1)])
        df = UserInfoMultipleExtractor.get_age_and_name_features(ts, age, name)
        self.assertEqual(df.shape, (2, 3))
        self.assertEqual(
            df["UserInfoMultipleExtractor.age_squared"].tolist(), [1024, 576]
        )
        self.assertEqual(
            df["UserInfoMultipleExtractor.age_cubed"].tolist(), [32768, 13824]
        )
        self.assertEqual(
            df["UserInfoMultipleExtractor.is_name_common"].tolist(),
            [True, False],
        )

    @pytest.mark.integration
    @mock_client
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
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        ts = pd.Series([now, now])
        user_ids = pd.Series([18232, 18234])
        df = UserInfoSingleExtractor.get_user_info(ts, user_ids)
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

        series = UserInfoMultipleExtractor.get_country_geoid(ts, user_ids)
        assert series.tolist() == [5, 3]


class TestExtractorDAGResolution(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_dag_resolution(self, client):
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
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
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
            input_df=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))

        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoMultipleExtractor,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_df=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))

        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoMultipleExtractor,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_df=pd.DataFrame(
                {UserInfoMultipleExtractor.userid: [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))


@meta(owner="test@test.com")
@featureset
class UserInfoTransformedFeatures:
    age_power_four: int = feature(id=1)
    is_name_common: bool = feature(id=2)
    country_geoid_square: int = feature(id=3)

    @classmethod
    @extractor
    def get_user_transformed_features(
            cls,
            ts: Series[datetime],
            user_features: DataFrame[UserInfoMultipleExtractor],
    ):
        age = user_features["UserInfoMultipleExtractor.age"]
        is_name_common = user_features[
            "UserInfoMultipleExtractor.is_name_common"
        ]
        age_power_four = age ** 4
        country_geoid = (
                user_features["UserInfoMultipleExtractor.country_geoid"] ** 2
        )
        return pd.DataFrame(
            {
                "age_power_four": age_power_four,
                "is_name_common": is_name_common,
                "country_geoid_square": country_geoid,
            }
        )


class TestExtractorDAGResolutionComplex(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_dag_resolution_complex(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[
                UserInfoMultipleExtractor,
                UserInfoTransformedFeatures,
            ],
        )
        now = datetime.now()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        feature_df = client.extract_features(
            output_feature_list=[
                UserInfoTransformedFeatures,
            ],
            input_feature_list=[UserInfoMultipleExtractor.userid],
            input_df=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 3))
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

    @classmethod
    @extractor
    @depends_on(DocumentContentDataset)
    def get_doc_features(
            cls, ts: Series[datetime], doc_id: Series[doc_id]
    ) -> DataFrame[num_words, bert_embedding, fast_text_embedding]:
        df, _ = DocumentContentDataset.lookup(ts, doc_id=doc_id)  # type: ignore
        return df[["bert_embedding", "fast_text_embedding", "num_words"]]


class TestDocumentDataset(unittest.TestCase):
    @mock_client
    def test_document_featureset(self, client):
        client.sync(
            datasets=[DocumentContentDataset], featuresets=[DocumentFeatures]
        )
        now = datetime.now()
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
        response = client.log("DocumentContentDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        feature_df = client.extract_features(
            output_feature_list=[
                DocumentFeatures,
            ],
            input_feature_list=[DocumentFeatures.doc_id],
            input_df=pd.DataFrame({DocumentFeatures.doc_id: [18232, 18234]}),
        )
        self.assertEqual(feature_df.shape, (2, 4))
