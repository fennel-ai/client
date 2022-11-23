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
        df["userid"] = user_id
        df["age_squared"] = df["age"] ** 2
        df["age_cubed"] = df["age"] ** 3
        df["is_name_common"] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            ["userid", "age", "age_squared", "age_cubed", "is_name_common"]
        ]


def get_country_geoid(country: str) -> Tuple[int, int]:
    if country == "Russia":
        return 1, 2
    elif country == "Chile":
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
    def get_user_age_and_name(
        ts: pd.Series, user_id: Series[userid]
    ) -> DataFrame[age, name]:
        df = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df[["age", "name"]]

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
    ) -> Series[country_geoid]:
        df = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        return df["country"].apply(get_country_geoid)


class TestSimpleExtractor(unittest.TestCase):
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

    @mock_client
    def test_simple_extractor(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[UserInfoSingleExtractor, UserInfoMultipleExtractor],
        )
        data = [
            [18232, "John", 32, "USA", 1010],
            [18234, "Monica", 24, "Chile", 1010],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()
        ts = pd.Series([1011, 1012])
        user_ids = pd.Series([18232, 18234])
        df = UserInfoSingleExtractor.get_user_info(ts, user_ids)
        self.assertEqual(df.shape, (2, 5))
        self.assertEqual(
            df["UserInfoSingleExtractor.userid"].tolist(), [18232, 18234]
        )
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
        assert series.tolist() == [(5, 6), (3, 4)]


class TestExtractorDAGResolution(unittest.TestCase):
    @mock_client
    def test_dag_resolution(self, client):
        client.sync(
            datasets=[UserInfoDataset],
            featuresets=[UserInfoSingleExtractor, UserInfoMultipleExtractor],
        )
        data = [
            [18232, "John", 32, "USA", 1010],
            [18234, "Monica", 24, "Chile", 1010],
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
            timestamps=pd.Series([1011, 1012]),
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
            timestamps=pd.Series([1011, 1012]),
        )
        self.assertEqual(feature_df.shape, (2, 7))
        #
        # feature_df = client.extract_features(
        #     output_feature_list=[
        #         UserInfoMultipleExtractor,
        #     ],
        #     input_feature_list=[UserInfoMultipleExtractor.userid],
        #     input_df=pd.DataFrame(
        #         {UserInfoMultipleExtractor.userid: [18232, 18234]}),
        #     timestamps=pd.Series([1011, 1012]),
        # )
        # self.assertEqual(feature_df.shape, (2, 7))
