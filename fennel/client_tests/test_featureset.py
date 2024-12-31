import sys
import unittest
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
from fennel.lib import meta

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
from fennel.expr import col, lit, when, repeat
from fennel.testing import mock, log

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
    hobbies: List[Optional[str]]
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
    age_reciprocal_expr: float = F(1 / (col("age") / (3600.0 * 24)) + 0.01)
    age_double_expr: int = F(col("age") * 2)
    age_greater_than_30: bool = F(
        when(col("age") > 30).then(lit(True)).otherwise(lit(False))
    )

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
            "UserInfoMultipleExtractor.age_reciprocal_expr",
            "UserInfoMultipleExtractor.age_double_expr",
            "UserInfoMultipleExtractor.age_greater_than_30",
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
            [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
            [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
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

    @pytest.mark.integration
    @mock
    def test_e2e_query(self, client):
        client.commit(
            message="some commit msg",
            datasets=[UserInfoDataset],
            featuresets=[UserInfoMultipleExtractor],
        )
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
            [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
        input_df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", input_df)
        assert response.status_code == requests.codes.OK, response.json()
        client.sleep()

        feature_df = client.query(
            outputs=[UserInfoMultipleExtractor],
            inputs=[UserInfoMultipleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoMultipleExtractor.userid": [18232, 18234]}
            ),
        )
        assert feature_df.shape == (2, 12)
        assert feature_df.columns.tolist() == [
            "UserInfoMultipleExtractor.userid",
            "UserInfoMultipleExtractor.name",
            "UserInfoMultipleExtractor.country_geoid",
            "UserInfoMultipleExtractor.age",
            "UserInfoMultipleExtractor.age_squared",
            "UserInfoMultipleExtractor.age_cubed",
            "UserInfoMultipleExtractor.is_name_common",
            "UserInfoMultipleExtractor.age_reciprocal",
            "UserInfoMultipleExtractor.age_doubled",
            "UserInfoMultipleExtractor.age_reciprocal_expr",
            "UserInfoMultipleExtractor.age_double_expr",
            "UserInfoMultipleExtractor.age_greater_than_30",
        ]
        assert feature_df["UserInfoMultipleExtractor.userid"].tolist() == [
            18232,
            18234,
        ]

        assert feature_df["UserInfoMultipleExtractor.age"].tolist() == [32, 24]
        assert feature_df["UserInfoMultipleExtractor.age_squared"].tolist() == [
            1024,
            576,
        ]
        assert feature_df["UserInfoMultipleExtractor.age_cubed"].tolist() == [
            32768,
            13824,
        ]
        assert feature_df[
            "UserInfoMultipleExtractor.is_name_common"
        ].tolist() == [
            True,
            False,
        ]
        expected_age_reciprocal = [2700.01, 3600.01]
        assert (
            feature_df["UserInfoMultipleExtractor.age_reciprocal"].tolist()
            == expected_age_reciprocal
        )
        assert feature_df["UserInfoMultipleExtractor.age_doubled"].tolist() == [
            64,
            48,
        ]
        assert feature_df[
            "UserInfoMultipleExtractor.country_geoid"
        ].tolist() == [5, 3]
        assert feature_df["UserInfoMultipleExtractor.name"].tolist() == [
            "John",
            "Monica",
        ]
        assert (
            feature_df["UserInfoMultipleExtractor.age_reciprocal_expr"].tolist()
            == expected_age_reciprocal
        )
        assert feature_df[
            "UserInfoMultipleExtractor.age_double_expr"
        ].tolist() == [64, 48]
        assert feature_df[
            "UserInfoMultipleExtractor.age_greater_than_30"
        ].tolist() == [True, False]


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
            [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
            [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        ]
        df = pd.DataFrame(
            data,
            columns=[
                "user_id",
                "name",
                "age",
                "country",
                "timestamp",
                "hobbies",
            ],
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
            [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
            [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
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
        self.assertEqual(feature_df.shape, (2, 12))
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_reciprocal"]),
            [2700.01, 3600.01],
        )
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_doubled"]),
            [64, 48],
        )
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_reciprocal_expr"]),
            [2700.01, 3600.01],
        )
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_double_expr"]),
            [64, 48],
        )
        self.assertEqual(
            list(feature_df["UserInfoMultipleExtractor.age_greater_than_30"]),
            [True, False],
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
            [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
            [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
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


@pytest.mark.skipif(
    sys.version_info[:2] <= (3, 10),
    reason="Optional embedding not supported in python 3.9/10",
)
@mock
def test_embedding_features(client):
    @dataset(version=4, index=True)
    class ImageEmbeddings:
        image_id: int = field(key=True)
        embedding: Embedding[2]
        ts: datetime

    @featureset
    class ImageFeature:
        image_id: int
        embedding: Optional[Embedding[2]] = F(ImageEmbeddings.embedding)

    @featureset
    class ImageFeatureWithDefault:
        image_id: int = F(ImageFeature.image_id)
        embedding2: Embedding[2] = F(
            ImageEmbeddings.embedding, default=[11.0, 13.2]
        )

    client.commit(
        datasets=[ImageEmbeddings],
        featuresets=[ImageFeature, ImageFeatureWithDefault],
        message="committing image embeddings",
    )

    log(
        ImageEmbeddings,
        df=pd.DataFrame(
            {
                "image_id": [1, 2, 3],
                "embedding": [[1.0, 2.0], [2.0, 3.0], [3.0, 4.0]],
                "ts": [
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                ],
            }
        ),
    )

    feature_df = client.query(
        outputs=[ImageFeature, ImageFeatureWithDefault],
        inputs=[ImageFeature.image_id],
        input_dataframe=pd.DataFrame({"ImageFeature.image_id": [1, 2, 4]}),
    )
    assert feature_df.shape == (3, 4)
    assert feature_df["ImageFeature.embedding"].tolist() == [
        [1.0, 2.0],
        [2.0, 3.0],
        pd.NA,
    ]
    assert feature_df["ImageFeatureWithDefault.embedding2"].tolist() == [
        [1.0, 2.0],
        [2.0, 3.0],
        [11.0, 13.2],
    ]


@featureset
class Request:
    user_id: int


@source(webhook.endpoint("CountryInfo"), disorder="14d", cdc="upsert")
@dataset(index=True)
class CountryInfo:
    country: str = field(key=True)
    national_sport: str
    ts: datetime


@source(webhook.endpoint("Sport"), disorder="14d", cdc="upsert")
@dataset(index=True)
class Sport:
    sport: str = field(key=True)
    num_players: int
    ts: datetime


@featureset
class UserInfo:
    user_id: int = F(Request.user_id)
    country: Optional[str] = F(UserInfoDataset.country)
    sport: Optional[str] = F(CountryInfo.national_sport)
    num_players: Optional[int] = F(Sport.num_players)


@featureset
class UserInfo2:
    user_id: int = F(Request.user_id)
    country: Optional[str] = F(UserInfoDataset.country)
    sport: str = F(CountryInfo.national_sport, default="Cricket")
    num_players: Optional[int] = F(Sport.num_players)


@featureset
class UserInfo3:
    user_id: int = F(Request.user_id)
    country: Optional[str] = F(UserInfoDataset.country)
    sport: str = F(CountryInfo.national_sport, default="Rugby")
    num_players: Optional[int] = F(Sport.num_players)


@pytest.mark.integration
@mock
def test_chained_lookups(client):
    client.commit(
        datasets=[CountryInfo, Sport, UserInfoDataset],
        featuresets=[Request, UserInfo, UserInfo2, UserInfo3],
        message="Initial commit",
    )

    now = datetime.now(timezone.utc)
    data = [
        [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
        [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        [18235, "Rahul", 28, "India", now, ["Reading", "Football"]],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
    input_df = pd.DataFrame(data, columns=columns)
    client.log("fennel_webhook", "UserInfoDataset", input_df)

    client.log(
        "fennel_webhook",
        "CountryInfo",
        pd.DataFrame(
            {
                "country": ["USA", "Chile", "India"],
                "national_sport": ["Baseball", "Football", "Hockey"],
                "ts": [now, now, now],
            }
        ),
    )

    client.log(
        "fennel_webhook",
        "Sport",
        pd.DataFrame(
            {
                "sport": ["Baseball", "Football", "Hockey", "Cricket"],
                "num_players": [9, 11, 6, 20],
                "ts": [now, now, now, now],
            }
        ),
    )

    client.sleep()

    feature_df = client.query(
        outputs=[UserInfo],
        inputs=[Request.user_id],
        input_dataframe=pd.DataFrame(
            {"Request.user_id": [18232, 18234, 18235, 19000]}
        ),
    )

    print(feature_df)

    assert feature_df.shape == (4, 4)
    assert feature_df["UserInfo.country"].tolist() == [
        "USA",
        "Chile",
        "India",
        pd.NA,
    ]
    assert feature_df["UserInfo.sport"].tolist() == [
        "Baseball",
        "Football",
        "Hockey",
        pd.NA,
    ]
    assert feature_df["UserInfo.num_players"].tolist() == [9, 11, 6, pd.NA]

    feature_df = client.query(
        outputs=[UserInfo2],
        inputs=[Request.user_id],
        input_dataframe=pd.DataFrame(
            {"Request.user_id": [18232, 18234, 18235, 19000]}
        ),
    )
    print(feature_df)

    assert feature_df.shape == (4, 4)
    assert feature_df["UserInfo2.country"].tolist() == [
        "USA",
        "Chile",
        "India",
        pd.NA,
    ]
    assert feature_df["UserInfo2.sport"].tolist() == [
        "Baseball",
        "Football",
        "Hockey",
        "Cricket",
    ]
    assert feature_df["UserInfo2.num_players"].tolist() == [9, 11, 6, 20]

    feature_df = client.query(
        outputs=[UserInfo3],
        inputs=[Request.user_id],
        input_dataframe=pd.DataFrame(
            {"Request.user_id": [18232, 18234, 18235, 19000]}
        ),
    )

    assert feature_df.shape == (4, 4)
    assert feature_df["UserInfo3.country"].tolist() == [
        "USA",
        "Chile",
        "India",
        pd.NA,
    ]
    assert feature_df["UserInfo3.sport"].tolist() == [
        "Baseball",
        "Football",
        "Hockey",
        "Rugby",
    ]
    assert feature_df["UserInfo3.num_players"].tolist() == [9, 11, 6, pd.NA]


@pytest.mark.integration
@mock
def test_auto_extractor_removal(client):
    @source(webhook.endpoint("IndexDataset"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class IndexDataset:
        user_id: int = field(key=True)
        name: str
        age: int
        timestamp: datetime

    @featureset
    class IndexFeatures:
        user_id: int
        name: Optional[str] = F(IndexDataset.name)
        age: Optional[int] = F(IndexDataset.age)

    response = client.commit(
        datasets=[IndexDataset],
        featuresets=[IndexFeatures],
        message="first_commit",
    )
    assert response.status_code == requests.codes.OK, response.json()

    # Try removing one auto extractor from incremental mode
    def remove_age():
        @featureset
        class IndexFeatures:
            user_id: int
            name: Optional[str] = F(IndexDataset.name)
            age: Optional[int] = F(IndexDataset.age).meta(deleted=True)

        return IndexFeatures

    client.commit(
        featuresets=[remove_age()], message="second_commit", incremental=True
    )
    assert response.status_code == requests.codes.OK, response.json()

    # Try removing one auto extractor from normal commit
    def remove_name():
        @featureset
        class IndexFeatures:
            user_id: int
            name: Optional[str] = F(IndexDataset.name).meta(deleted=True)
            age: Optional[int] = F(IndexDataset.age).meta(deleted=True)

        return IndexFeatures

    client.commit(
        datasets=[IndexDataset],
        featuresets=[remove_name()],
        message="third_commit",
    )
    assert response.status_code == requests.codes.OK, response.json()


@mock
def test_query_time_features(client):
    @meta(owner="decision-systems@theporter.in")
    @featureset
    class QueryTimeFeatures:
        query_time: datetime
        hour_utc: int = F(col("query_time").dt.hour)
        bucket_3hour_utc: int = F((col("hour_utc") / lit(3)).floor() + lit(1))
        bucket_6hour_utc: int = F((col("hour_utc") / lit(6)).floor() + lit(1))
        day_utc: int = F(col("query_time").dt.day)
        day_of_week_utc: int
        week_of_month_utc: int = F((col("day_utc") / lit(7)).ceil())
        week_bucket_utc: int

        @extractor(version=1)
        @outputs("query_time")
        def current_time(cls, ts: pd.Series):
            return pd.Series(name="query_time", data=ts)

        @extractor(version=8)
        @outputs(
            "day_of_week_utc",
            "week_bucket_utc",
        )
        def time_feature_extractor(cls, ts: pd.Series) -> pd.DataFrame:
            pass

    client.commit(
        featuresets=[QueryTimeFeatures],
        message="first_commit",
    )


@pytest.mark.integration
@mock
def test_looks_like_list_lookup(client):
    @featureset
    class UserInfoListLookup:
        user_id: int
        hobbies: List[Optional[str]] = F(
            UserInfoDataset.hobbies, default=["Unknown"]
        )

    client.commit(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoListLookup],
        message="Initial commit",
    )

    now = datetime.now(timezone.utc)
    data = [
        [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
        [18234, "Monica", 24, "Chile", now, ["Reading", "Writing"]],
        [18235, "Rahul", 28, "India", now, ["Reading", "Writing"]],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
    input_df = pd.DataFrame(data, columns=columns)
    client.log("fennel_webhook", "UserInfoDataset", input_df)

    client.sleep()

    feature_df = client.query(
        outputs=[UserInfoListLookup],
        inputs=[UserInfoListLookup.user_id],
        input_dataframe=pd.DataFrame(
            {"UserInfoListLookup.user_id": [18234, 18235]}
        ),
    )

    assert feature_df.shape == (2, 2)
    assert feature_df["UserInfoListLookup.hobbies"].tolist() == [
        ["Reading", "Writing"],
        ["Reading", "Writing"],
    ]


@pytest.mark.integration
@mock
def test_list_lookup(client):
    @featureset
    class UserInfoListLookup:
        user_id: List[int]
        country: List[Optional[str]] = F(UserInfoDataset.country)
        name: List[str] = F(UserInfoDataset.name, default="Unknown")

    @featureset
    class UserInfoListLookup2:
        user_id_request: int = F(Request.user_id)
        user_id: List[int] = F(repeat(col("user_id_request"), 5))
        country: List[Optional[str]] = F(UserInfoDataset.country)
        name: List[str] = F(UserInfoDataset.name, default="Unknown")

    client.commit(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoListLookup, UserInfoListLookup2, Request],
        message="Initial commit",
    )
    client.sleep()

    now = datetime.now(timezone.utc)
    data = [
        [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
        [18234, "Monica", 24, "Chile", now, ["Reading", "Writing"]],
        [18235, "Rahul", 28, "India", now, ["Reading", "Writing"]],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
    input_df = pd.DataFrame(data, columns=columns)
    client.log("fennel_webhook", "UserInfoDataset", input_df)

    client.sleep()

    feature_df = client.query(
        outputs=[UserInfoListLookup],
        inputs=[UserInfoListLookup.user_id],
        input_dataframe=pd.DataFrame(
            {
                "UserInfoListLookup.user_id": [
                    [18232, 18234, 18235, 123],
                    [18232, 18234],
                    [18234, 18235, 1],
                ]
            }
        ),
    )

    assert feature_df.shape == (3, 3)
    assert feature_df["UserInfoListLookup.country"].tolist() == [
        ["USA", "Chile", "India", pd.NA],
        ["USA", "Chile"],
        ["Chile", "India", pd.NA],
    ]
    assert feature_df["UserInfoListLookup.name"].tolist() == [
        ["John", "Monica", "Rahul", "Unknown"],
        ["John", "Monica"],
        ["Monica", "Rahul", "Unknown"],
    ]

    feature_df2 = client.query(
        outputs=[UserInfoListLookup2],
        inputs=[Request.user_id],
        input_dataframe=pd.DataFrame({"Request.user_id": [18232, 12345]}),
    )

    assert feature_df2.shape == (2, 4)
    assert feature_df2["UserInfoListLookup2.country"].tolist() == [
        ["USA", "USA", "USA", "USA", "USA"],
        [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
    ]


@pytest.mark.integration
@mock
def test_chainedlist_lookup_with_default(client):
    """_summary_
    In this test, we look up a list of categories from a user id, and then look up a list of values from the categories.
    """

    @source(webhook.endpoint("HobbyDataset"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class HobbyDataset:
        hobby: str = field(key=True)
        category: str
        ts: datetime

    @featureset
    class UserHobbies:
        user_id: int
        hobby: List[Optional[str]] = F(UserInfoDataset.hobbies, default=[])
        categories: List[Optional[str]] = F(
            HobbyDataset.category,
        )

    client.commit(
        datasets=[HobbyDataset, UserInfoDataset],
        featuresets=[UserHobbies],
        message="Initial commit",
    )

    client.sleep()
    now = datetime.now(timezone.utc)
    data = [
        [18232, "John", 32, "USA", now, ["Reading", "Writing"]],
        [18234, "Monica", 24, "Chile", now, ["Cooking", "Music"]],
        [18235, "Rahul", 28, "India", now, ["Reading", "Football"]],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp", "hobbies"]
    input_df = pd.DataFrame(data, columns=columns)
    client.log("fennel_webhook", "UserInfoDataset", input_df)

    client.sleep()

    data = [
        ["Reading", "Productivity", now],
        ["Football", "Sports", now],
        ["Cooking", "Food", now],
    ]
    columns = ["hobby", "category", "ts"]
    input_df = pd.DataFrame(data, columns=columns)
    client.log("fennel_webhook", "HobbyDataset", input_df)

    client.sleep()

    feature_df = client.query(
        outputs=[UserHobbies],
        inputs=[UserHobbies.user_id],
        input_dataframe=pd.DataFrame(
            {
                "UserHobbies.user_id": [18232, 18234, 18235],
            }
        ),
    )

    assert feature_df.shape == (3, 3)
    assert feature_df["UserHobbies.hobby"].tolist() == [
        ["Reading", "Writing"],
        ["Cooking", "Music"],
        ["Reading", "Football"],
    ]
    assert feature_df["UserHobbies.categories"].tolist() == [
        ["Productivity", pd.NA],
        ["Food", pd.NA],
        ["Productivity", "Sports"],
    ]


@pytest.mark.integration
@mock
def test_multiple_keyed_list_lookup(client):
    @source(webhook.endpoint("KeyedDataset"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class KeyedDataset:
        key1: int = field(key=True)
        key2: int = field(key=True)
        value: str
        ts: datetime = field(timestamp=True)

    @featureset
    class KeyedFeatureset:
        key1: List[int]
        key2: List[int]
        value: List[Optional[str]] = F(KeyedDataset.value)

    client.commit(
        datasets=[KeyedDataset], featuresets=[KeyedFeatureset], message="msg"
    )
    client.sleep()
    # Log data
    now = datetime.now(timezone.utc)
    data = [
        [1, 2, "a", now],
        [3, 4, "b", now],
        [5, 6, "c", now],
        [7, 8, "d", now],
    ]
    columns = ["key1", "key2", "value", "ts"]
    input_df = pd.DataFrame(data, columns=columns)
    client.log("fennel_webhook", "KeyedDataset", input_df)

    client.sleep()

    feature_df = client.query(
        inputs=[KeyedFeatureset.key1, KeyedFeatureset.key2],
        outputs=[KeyedFeatureset.value],
        input_dataframe=pd.DataFrame(
            {
                "KeyedFeatureset.key1": [[1, 2, 3], [7, 5, 6]],
                "KeyedFeatureset.key2": [[2, 8, 4], [8, 11, 12]],
            }
        ),
    )
    assert feature_df.shape == (2, 1)
    assert feature_df["KeyedFeatureset.value"].tolist() == [
        ["a", pd.NA, "b"],
        ["d", pd.NA, pd.NA],
    ]


@pytest.mark.integration
@mock
def test_alias_extractor_with_default_value(client):
    @source(webhook.endpoint("KeyedDataset"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class KeyedDataset:
        key: int = field(key=True)
        value1: str
        value2: str
        ts: datetime = field(timestamp=True)

    @featureset
    class KeyedFS:
        key: int
        value1: Optional[str] = F(KeyedDataset.value1)
        value2: Optional[str] = F(KeyedDataset.value2)

    @featureset
    class KeyedFSWithDefault:
        value1: str = F(KeyedFS.value1, default="default_value")
        value2: Optional[str] = F(KeyedFS.value2)

    client.commit(
        datasets=[KeyedDataset],
        featuresets=[KeyedFS, KeyedFSWithDefault],
        message="msg",
    )
    client.sleep()

    now = datetime.now(timezone.utc)
    input_df = pd.DataFrame(
        {
            "key": [1, 2, 3, 4],
            "value1": ["a", "b", "c", "d"],
            "value2": ["aa", "bb", "cc", "dd"],
            "ts": [now, now, now, now],
        }
    )
    client.log("fennel_webhook", "KeyedDataset", input_df)
    client.sleep()

    feature_df = client.query(
        inputs=[KeyedFS.key],
        outputs=[KeyedFSWithDefault],
        input_dataframe=pd.DataFrame(
            {
                "KeyedFS.key": [1, 2, 3, 4, 5],
            }
        ),
    )
    assert feature_df.shape == (5, 2)
    assert feature_df["KeyedFSWithDefault.value1"].tolist() == [
        "a",
        "b",
        "c",
        "d",
        "default_value",
    ]
    assert feature_df["KeyedFSWithDefault.value2"].tolist() == [
        "aa",
        "bb",
        "cc",
        "dd",
        pd.NA,
    ]
