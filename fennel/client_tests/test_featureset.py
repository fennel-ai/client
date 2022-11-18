import unittest
from datetime import datetime
from typing import Optional
from typing import Tuple

import pandas as pd
import requests

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import mock_client


################################################################################
#                           Feature Single Extracator Unit Tests
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
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    @depends_on(UserInfoDataset)
    def get_user_info(ts: pd.Series, user_id: Series[userid]):
        df = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df["age_squared"] = df["age"] ** 2
        df["age_cubed"] = df["age"] ** 3
        df["is_name_common"] = df["name"].isin(["John", "Mary", "Bob"])
        return df


def get_country_geoid(country: str) -> Tuple[int, int]:
    if country == "US":
        return 1, 2
    elif country == "UK":
        return 3, 4
    else:
        return 5, 6


@meta(owner="test@test.com")
@featureset
class UserInfoMultipleExtractor:
    userid: int = feature(id=1)
    name: str = feature(id=2)
    country_geoid: int = feature(id=3).meta(wip=True)  # type: ignore
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    @depends_on(UserInfoDataset)
    def get_user_age(ts: pd.Series, user_id: Series[userid]) -> Series[age]:
        df = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df["age"]

    @extractor
    def get_age_and_name_features(
        ts: pd.Series, user_age: Series[age], name: Series[name]
    ) -> DataFrame[age_squared, age_cubed, is_name_common]:
        is_name_common = name.isin(["John", "Mary", "Bob"])
        return pd.concat([user_age**2, user_age**3, is_name_common], axis=1)

    @extractor
    @depends_on(UserInfoDataset)
    def get_country_geoid(
        ts: pd.Series, user_id: Series[userid]
    ) -> DataFrame[country_geoid]:
        df = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df["country"].apply(get_country_geoid)


class TestSimpleExtractor(unittest.TestCase):
    def test_get_age_and_name_features(self):
        age = pd.Series([32, 24])
        name = pd.Series(["John", "Mary"])
        ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 1)])
        df = UserInfoMultipleExtractor.get_age_and_name_features(ts, age, name)
        self.assertEqual(df.shape, (2, 3))

    @mock_client
    def test_simple_extractor(self, client):
        client.sync(datasets=[UserInfoDataset])
        data = [
            [18232, "Ross", 32, "USA", 1668475993],
            [18234, "Monica", 24, "Chile", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
