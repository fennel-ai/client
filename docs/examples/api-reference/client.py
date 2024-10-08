import unittest
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.featuresets import feature as F, featureset, extractor
from fennel.lib import includes, meta, inputs, outputs
from fennel.testing import mock
from fennel.expr.expr import col, lit, when
from fennel.client import Client

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def get_country_geoid(country: str) -> int:
    if country == "Russia":
        return 1
    elif country == "Chile":
        return 3
    else:
        return 5


@meta(owner="test@test.com")
@featureset
class UserFeatures:
    userid: int
    name: str
    country_geoid: int
    # The users gender among male/female/non-binary
    age: int = F().meta(owner="aditya@fennel.ai")
    age_squared: int
    age_cubed: int
    is_name_common: bool
    item_claim_ctr: float = F(
        when(col("age_cubed") > 0).then(lit(1.0)).otherwise(0.0)
    )

    @extractor(deps=[UserInfoDataset])
    @inputs("userid")
    @outputs("age", "name")
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)
        return df[["age", "name"]]

    @extractor
    @inputs(age, "name")
    @outputs("age_squared", "age_cubed", "is_name_common")
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        is_name_common = name.isin(["John", "Mary", "Bob"])
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            "age_squared",
            "age_cubed",
            "is_name_common",
        ]
        return df

    @extractor(deps=[UserInfoDataset])
    @includes(get_country_geoid)
    @inputs("userid")
    @outputs("country_geoid")
    def get_country_geoid_extractor(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["country_geoid"] = df["country"].apply(get_country_geoid)
        return df[["country_geoid"]]


# this is your test code in some test module
class TestExtractorDAGResolution(unittest.TestCase):
    @mock
    def test_dag_resolution(self, client):
        # docsnip commit_api
        client.commit(
            message="add user info dataset and user features",
            datasets=[UserInfoDataset],
            featuresets=[UserFeatures],
        )
        # /docsnip
        # docsnip log_api
        now = datetime.now(timezone.utc)
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip

        # docsnip query_api
        feature_df = client.query(
            outputs=[UserFeatures],
            inputs=[UserFeatures.userid],
            input_dataframe=pd.DataFrame(
                {"UserFeatures.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 8))
        # /docsnip

        # docsnip query_offline_api
        response = client.query_offline(
            outputs=[UserFeatures],
            inputs=[UserFeatures.userid],
            input_dataframe=pd.DataFrame(
                {"UserFeatures.userid": [18232, 18234], "timestamp": [now, now]}
            ),
            timestamp_column="timestamp",
        )
        # /docsnip

        # docsnip lookup
        response = client.lookup(
            dataset="UserInfoDataset",
            keys=pd.DataFrame({"user_id": [18232]}),
            fields=["name"],
        )
        # /docsnip

        with self.assertRaises(ValueError) as e:
            # docsnip query_offline_s3
            from fennel.connectors import S3

            s3 = S3(
                name="extract_hist_input",
                aws_access_key_id="<ACCESS KEY HERE>",
                aws_secret_access_key="<SECRET KEY HERE>",
            )
            s3_input_connection = s3.bucket(
                "bucket", prefix="data/user_features"
            )
            s3_output_connection = s3.bucket("bucket", prefix="output")

            response = client.query_offline(
                outputs=[UserFeatures],
                inputs=[UserFeatures.userid],
                timestamp_column="timestamp",
                input_s3=s3_input_connection,
                output_s3=s3_output_connection,
            )
            # /docsnip
        assert (
            "input must contain a key 'input_dataframe' with the input dataframe"
            in str(e.exception)
        )
