import unittest
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from fennel.datasets import dataset, field
from fennel.featuresets import feature, featureset, extractor
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.test_lib import mock_client


@meta(owner="test@test.com")
@dataset
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
    userid: int = feature(id=1)
    name: str = feature(id=2)
    country_geoid: int = feature(id=3)
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor(depends_on=[UserInfoDataset])
    @inputs(userid)
    @outputs(age, name)
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)
        return df[["age", "name"]]

    @extractor
    @inputs(age, name)
    @outputs(age_squared, age_cubed, is_name_common)
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        is_name_common = name.isin(["John", "Mary", "Bob"])
        df = pd.concat([user_age**2, user_age**3, is_name_common], axis=1)
        df.columns = [
            str(cls.age_squared),
            str(cls.age_cubed),
            str(cls.is_name_common),
        ]
        return df

    @extractor(depends_on=[UserInfoDataset])
    @inputs(userid)
    @outputs(country_geoid)
    def get_country_geoid_extractor(cls, ts: pd.Series, user_id: pd.Series):
        df, _found = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["country_geoid"] = df["country"].apply(get_country_geoid)
        return df[["country_geoid"]]


# this is your test code in some test module
class TestExtractorDAGResolution(unittest.TestCase):
    @mock_client
    def test_dag_resolution(self, client):
        # docsnip sync_api
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[UserFeatures],
        )
        # /docsnip
        # docsnip log_api
        now = datetime.now()
        data = [
            [18232, "John", 32, "USA", now],
            [18234, "Monica", 24, "Chile", now],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        # /docsnip

        # docsnip extract_features_api
        feature_df = client.extract_features(
            output_feature_list=[
                UserFeatures,
            ],
            input_feature_list=[UserFeatures.userid],
            input_dataframe=pd.DataFrame(
                {"UserFeatures.userid": [18232, 18234]}
            ),
        )
        self.assertEqual(feature_df.shape, (2, 7))
        # /docsnip
