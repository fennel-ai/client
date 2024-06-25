import unittest
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List

import numpy as np
import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.dtypes import Embedding, struct
from fennel.featuresets import featureset, extractor, feature as F
from fennel.lib import (
    includes,
    inputs,
    outputs,
    expectations,
    expect_column_values_to_be_between,
)
from fennel.testing import mock

################################################################################
#                           Feature Single Extractor Unit Tests
################################################################################

__owner__ = "test@test.com"
webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
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


@featureset
class UserInfoSingleExtractor:
    userid: int
    age: int = F().meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int
    age_cubed: int
    is_name_common: bool

    @extractor(deps=[UserInfoDataset])  # type: ignore
    @inputs("userid")
    @outputs("age", "age_squared", "age_cubed", "is_name_common")
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["userid"] = user_id
        df["age_squared"] = df["age"] ** 2
        df["age_cubed"] = df["age"] ** 3
        df["is_name_common"] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                "age",
                "age_squared",
                "age_cubed",
                "is_name_common",
            ]
        ]


def get_country_geoid(country: str) -> int:
    if country == "Russia":
        return 1
    elif country == "Chile":
        return 3
    else:
        return 5


@featureset
class UserInfoMultipleExtractor:
    userid: int
    name: str
    country_geoid: int
    age: int = F().meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int
    age_cubed: int
    is_name_common: bool
    age_reciprocal: float
    age_doubled: int

    @extractor(deps=[UserInfoDataset])  # type: ignore
    @inputs("userid")
    @outputs("age", "name")
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df[["age", "name"]]

    @extractor
    @inputs("age", "name")
    @outputs("age_squared", "age_cubed", "is_name_common")
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        is_name_common = name.isin(cls.get_common_names())
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            "age_squared",
            "age_cubed",
            "is_name_common",
        ]
        return df

    @extractor
    @inputs("age")
    @outputs("age_reciprocal")
    def get_age_reciprocal(cls, ts: pd.Series, age: pd.Series):
        d = age.apply(lambda x: 1 / (x / (3600.0 * 24)) + 0.01)
        return pd.Series(name="age_reciprocal", data=d)

    @extractor
    @inputs("age")
    @outputs("age_doubled")
    def get_age_doubled(cls, ts: pd.Series, age: pd.Series):
        d2 = age.apply(lambda x: x * 2)
        d2.name = "age_doubled"
        d3 = age.apply(lambda x: x * 3)
        d3.name = "age_tripled"
        d4 = age.apply(lambda x: x * 4)
        d4.name = "age_quad"
        # returns a dataframe with extra columns
        return pd.DataFrame([d4, d3, d2]).T

    @extractor(deps=[UserInfoDataset], version=2)  # type: ignore
    @includes(get_country_geoid)
    @inputs("userid")
    @outputs("country_geoid")
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
        print("Running test_get_age_and_name_features")
        age = pd.Series([32, 24])
        name = pd.Series(["John", "Rahul"])
        assert UserInfoMultipleExtractor.all() == [
            "UserInfoMultipleExtractor.userid",
            "UserInfoMultipleExtractor.name",
            "UserInfoMultipleExtractor.country_geoid",
            "UserInfoMultipleExtractor.age",
            "UserInfoMultipleExtractor.age_squared",
            "UserInfoMultipleExtractor.age_cubed",
            "UserInfoMultipleExtractor.is_name_common",
            "UserInfoMultipleExtractor.age_reciprocal",
            "UserInfoMultipleExtractor.age_doubled",
        ]
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
        client.commit(
            message="some commit msg",
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", input_df)
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

        res = UserInfoMultipleExtractor.get_age_doubled(
            UserInfoMultipleExtractor, ts, input_df["age"]
        )
        self.assertEqual(res.shape, (2, 3))  # extractor has extra cols
        self.assertEqual(
            res["UserInfoMultipleExtractor.age_doubled"].tolist(), [64, 48]
        )


@struct
class Velocity:
    speed: float
    direction: int


@source(webhook.endpoint("FlightDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class FlightDataset:
    id: int = field(key=True)
    ts: datetime = field(timestamp=True)
    v_cruising: Velocity
    layout: Dict[str, Optional[int]]
    pilot_ids: List[int]
    region: str


@featureset
class FlightRequest:
    id: int


@featureset
class GeneratedFeatures:
    user_id: int = F(UserInfoSingleExtractor.userid)  # type: ignore
    id: int = F(FlightRequest.id)  # type: ignore
    country: str = F(UserInfoDataset.country, default="pluto")  # type: ignore
    velocity: Velocity = F(
        FlightDataset.v_cruising,  # type: ignore
        default=Velocity(500.0, 0),  # type: ignore
    )
    layout: Dict[str, Optional[int]] = F(
        FlightDataset.layout,  # type: ignore
        default={"economy": 0},
    )
    pilots: List[int] = F(
        FlightDataset.pilot_ids,  # type: ignore
        default=[0, 0, 0],
    )
    base_region: Optional[str] = F(
        FlightDataset.region,  # type: ignore
    )


class TestDerivedExtractor(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_derived_extractor(self, client):
        print("Running test_derived_extractor")
        client.commit(
            message="some commit msg",
            datasets=[UserInfoDataset, FlightDataset],
            featuresets=[
                UserInfoSingleExtractor,
                GeneratedFeatures,
                FlightRequest,
            ],
        )
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        df = pd.DataFrame(
            data, columns=["user_id", "name", "age", "country", "timestamp"]
        )
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        flight_data = {
            "id": [4032, 555],
            "ts": [now, now],
            "v_cruising": [Velocity(400, 180), Velocity(550, 90)],
            "layout": [
                {"economy": 180},
                {"first": 8, "business": 24, "economy": 300},
            ],
            "pilot_ids": [[1, 2], [3, 4, 5, 6]],
            "region": ["CA", "East"],
        }
        df = pd.DataFrame(flight_data)

        response = client.log("fennel_webhook", "FlightDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        feature_df = client.query(
            outputs=[
                UserInfoSingleExtractor.userid,
                GeneratedFeatures.user_id,
                GeneratedFeatures.country,
                GeneratedFeatures.velocity,
                GeneratedFeatures.layout,
                GeneratedFeatures.pilots,
                GeneratedFeatures.base_region,
            ],
            inputs=[
                UserInfoSingleExtractor.userid,
                FlightRequest.id,
            ],
            input_dataframe=pd.DataFrame(
                {
                    "UserInfoSingleExtractor.userid": [18232, 18234, 7],
                    "FlightRequest.id": [555, 1131, 4032],
                }
            ),
        )
        self.assertEqual(feature_df.shape, (3, 7))
        self.assertEqual(
            list(feature_df["UserInfoSingleExtractor.userid"]),
            [18232, 18234, 7],
        )
        self.assertEqual(
            list(feature_df["GeneratedFeatures.user_id"]), [18232, 18234, 7]
        )
        self.assertEqual(
            list(feature_df["GeneratedFeatures.country"]),
            ["USA", "Chile", "pluto"],
        )
        self.assertEqual(
            list(feature_df["GeneratedFeatures.velocity"]),
            [Velocity(550, 90), Velocity(500.0, 0), Velocity(400, 180)],
        )
        self.assertEqual(
            list(feature_df["GeneratedFeatures.layout"]),
            [
                {"first": 8, "business": 24, "economy": 300},
                {"economy": 0},
                {"economy": 180},
            ],
        )
        self.assertEqual(
            list(feature_df["GeneratedFeatures.pilots"]),
            [[3, 4, 5, 6], [0, 0, 0], [1, 2]],
        )
        self.assertEqual(
            list(feature_df["GeneratedFeatures.base_region"]),
            ["East", pd.NA, "CA"],
        )


class TestExtractorDAGResolution(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_dag_resolution2(self, client):
        print("Running test_dag_resolution2")
        client.commit(
            message="some commit msg",
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()
        feature_df = client.query(
            outputs=[
                UserInfoMultipleExtractor.userid,
                UserInfoMultipleExtractor.name,
                UserInfoMultipleExtractor.country_geoid,
                UserInfoMultipleExtractor.age,
                UserInfoMultipleExtractor.age_squared,
                UserInfoMultipleExtractor.age_cubed,
                UserInfoMultipleExtractor.is_name_common,
            ],
            inputs=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))

        feature_df = client.query(
            outputs=[UserInfoMultipleExtractor],
            inputs=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 9))
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_reciprocal"]),
            [2700.01, 3600.01],
        )


@featureset
class UserInfoTransformedFeatures:
    age_power_four: int
    is_name_common: bool
    country_geoid_square: int

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
                "age_power_four": age_power_four,
                "is_name_common": is_name_common,
                "country_geoid_square": country_geoid,
            }
        )


class TestExtractorDAGResolutionComplex(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_dag_resolution_complex(self, client):
        print("Running test_dag_resolution_complex")
        client.commit(
            message="some commit msg",
            datasets=[UserInfoDataset],
            featuresets=[
                UserInfoMultipleExtractor,
                UserInfoTransformedFeatures,
            ],
        )
        client.sleep()
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        feature_df = client.query(
            outputs=[
                UserInfoTransformedFeatures.age_power_four,
                UserInfoTransformedFeatures.is_name_common,
                UserInfoTransformedFeatures.country_geoid_square,
            ],
            inputs=[UserInfoMultipleExtractor.userid],
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

        feature_df = client.query_offline(
            outputs=[
                "UserInfoTransformedFeatures.age_power_four",
                "UserInfoTransformedFeatures.is_name_common",
                "UserInfoTransformedFeatures.country_geoid_square",
            ],
            inputs=["UserInfoMultipleExtractor.userid"],
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


@source(
    webhook.endpoint("DocumentContentDataset"), disorder="14d", cdc="upsert"
)
@dataset(index=True)
class DocumentContentDataset:
    doc_id: int = field(key=True)
    bert_embedding: Embedding[4]
    fast_text_embedding: Embedding[3]
    num_words: int
    timestamp: datetime = field(timestamp=True)


@featureset
class DocumentFeatures:
    doc_id: int
    bert_embedding: Embedding[4]
    fast_text_embedding: Embedding[3]
    num_words: int

    @extractor(deps=[DocumentContentDataset])  # type: ignore
    @inputs("doc_id")
    @outputs("num_words", "bert_embedding", "fast_text_embedding")
    def get_doc_features(cls, ts: pd.Series, doc_id: pd.Series):
        df, _ = DocumentContentDataset.lookup(ts, doc_id=doc_id)  # type: ignore

        df["bert_embedding"] = df["bert_embedding"].apply(
            lambda x: x if isinstance(x, (list, np.ndarray)) else [0, 0, 0, 0]
        )
        df["fast_text_embedding"] = df["fast_text_embedding"].apply(
            lambda x: x if isinstance(x, (list, np.ndarray)) else [0, 0, 0]
        )
        df["num_words"].fillna(0, inplace=True)
        return df[
            [
                "bert_embedding",
                "fast_text_embedding",
                "num_words",
            ]
        ]


class TestDocumentDataset(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_document_featureset(self, client):
        print("Running test_document_featureset")
        client.commit(
            message="some commit msg",
            datasets=[DocumentContentDataset],
            featuresets=[DocumentFeatures],
        )
        now = datetime.now(timezone.utc)
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
        feature_df = client.query(
            outputs=[DocumentFeatures],
            inputs=[DocumentFeatures.doc_id],
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

        embedding_resp = list(
            feature_df["DocumentFeatures.bert_embedding"].tolist()[0]
        )
        assert embedding_resp == [1, 2, 3, 4]

        if client.is_integration_client():
            return

        yesterday = datetime.now(timezone.utc) - timedelta(days=1)

        feature_df = client.query_offline(
            outputs=[DocumentFeatures],
            inputs=[DocumentFeatures.doc_id],
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


class TestOptionalTypes(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_optional_float_featureset(self, client):

        @source(webhook.endpoint("FloatDataset"), disorder="14d", cdc="upsert")
        @dataset(index=True)
        class FloatDataset:
            user_id: int = field(key=True)
            timestamp: datetime = field(timestamp=True)
            income: float

        @featureset
        class FloatFeatureSet:
            user_id: int
            income: Optional[float] = F(FloatDataset.income)

        client.commit(
            message="some commit msg",
            datasets=[FloatDataset],
            featuresets=[FloatFeatureSet],
        )

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "timestamp": [now, now, now, now, now],
                "income": [10000.1, 20000.0, 30000.6, 40000.0, 50000.2],
            }
        )
        client.log("fennel_webhook", "FloatDataset", df)

        client.sleep()
        feature_df = client.query(
            outputs=[FloatFeatureSet.income],
            inputs=[FloatFeatureSet.user_id],
            input_dataframe=pd.DataFrame(
                {"FloatFeatureSet.user_id": [1, 2, 3, 4, 5, 6]}
            ),
        )
        assert feature_df.shape == (6, 1)
        assert feature_df["FloatFeatureSet.income"].tolist() == [
            10000.1,
            20000.0,
            30000.6,
            40000.0,
            50000.2,
            pd.NA,
        ]

    @pytest.mark.integration
    @mock
    def test_optional_int_featureset(self, client):

        @source(webhook.endpoint("FloatDataset"), disorder="14d", cdc="upsert")
        @dataset(index=True)
        class IntDataset:
            user_id: int = field(key=True)
            timestamp: datetime = field(timestamp=True)
            age: int

        @featureset
        class IntFeatureSet:
            user_id: int
            age: Optional[int] = F(IntDataset.age)

        client.commit(
            message="some commit msg",
            datasets=[IntDataset],
            featuresets=[IntFeatureSet],
        )

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "timestamp": [now, now, now, now, now],
                "age": [10, 11, 12, 13, 14],
            }
        )
        client.log("fennel_webhook", "FloatDataset", df)

        client.sleep()
        feature_df = client.query(
            outputs=[IntFeatureSet.age],
            inputs=[IntFeatureSet.user_id],
            input_dataframe=pd.DataFrame(
                {"IntFeatureSet.user_id": [1, 2, 3, 4, 5, 6]}
            ),
        )
        assert feature_df.shape == (6, 1)
        assert feature_df["IntFeatureSet.age"].tolist() == [
            10,
            11,
            12,
            13,
            14,
            pd.NA,
        ]


@pytest.mark.integration
@mock
def test_featureset_name_query(client):
    """
    This tests the ability to provide the featureset name in outputs in query
    """

    @source(webhook.endpoint("DS1"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class DS1:
        user_id: int = field(key=True)
        timestamp: datetime = field(timestamp=True)
        age: int

    @featureset
    class FS1:
        user_id: int
        age: Optional[int] = F(DS1.age)

    client.commit(
        message="some commit msg",
        datasets=[DS1],
        featuresets=[FS1],
    )

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "timestamp": [now, now, now, now, now],
            "age": [10, 11, 12, 13, 14],
        }
    )
    client.log("fennel_webhook", "DS1", df)

    client.sleep()
    feature_df = client.query(
        outputs=["FS1"],
        inputs=[FS1.user_id],
        input_dataframe=pd.DataFrame({"FS1.user_id": [1, 2, 3, 4, 5, 6]}),
    )
    assert feature_df.shape == (6, 2)
    assert feature_df["FS1.age"].tolist() == [
        10,
        11,
        12,
        13,
        14,
        pd.NA,
    ]
