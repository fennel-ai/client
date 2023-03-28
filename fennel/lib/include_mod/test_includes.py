import time
from datetime import datetime
from typing import Optional

import pandas as pd
import pytest
import requests
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as fs_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.lib.include_mod import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import *


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def square(x: int) -> int:
    return x**2


def cube(x: int) -> int:
    return x**3


@includes(square)
def power_4(x: int) -> int:
    return square(square(x))


@meta(owner="test@test.com")
@featureset
class UserInfoSingleExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_power_four: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    @includes(power_4, cube)
    @depends_on(UserInfoDataset)
    def get_user_info(
        cls, ts: Series[datetime], user_id: Series[userid]
    ) -> DataFrame[age, age_power_four, age_cubed, is_name_common]:
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df[str(cls.userid)] = user_id
        df[str(cls.age_power_four)] = power_4(df["age"])
        df[str(cls.age_cubed)] = cube(df["age"])
        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                str(cls.age),
                str(cls.age_power_four),
                str(cls.age_cubed),
                str(cls.is_name_common),
            ]
        ]


def test_includes_proto_conversion(grpc_stub):
    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfoSingleExtractor)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 1
    assert len(sync_request.extractors) == 1
    assert len(sync_request.features) == 5
    extractor_proto = sync_request.extractors[0]
    f = {
        "name": "get_user_info",
        "datasets": ["UserInfoDataset"],
        "inputs": [
            {
                "feature": {
                    "featureSetName": "UserInfoSingleExtractor",
                    "name": "userid",
                }
            }
        ],
        "features": ["age", "age_power_four", "age_cubed", "is_name_common"],
        "metadata": {},
        "pycode": {
            "sourceCode": '    @extractor\n    @includes(power_4, cube)\n    @depends_on(UserInfoDataset)\n    def get_user_info(\n            cls, ts: Series[datetime], user_id: Series[userid]\n    ) -> DataFrame[age, age_power_four, age_cubed, is_name_common]:\n        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore\n        df[str(cls.userid)] = user_id\n        df[str(cls.age_power_four)] = power_4(df["age"])\n        df[str(cls.age_cubed)] = cube(df["age"])\n        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])\n        return df[\n            [\n                str(cls.age),\n                str(cls.age_power_four),\n                str(cls.age_cubed),\n                str(cls.is_name_common),\n            ]\n        ]\n',
            "name": "get_user_info",
            "includes": [
                {
                    "sourceCode": "@includes(square)\ndef power_4(x: int) -> int:\n    return square(square(x))\n",
                    "name": "power_4",
                    "includes": [
                        {
                            "sourceCode": "def square(x: int) -> int:\n    return x ** 2\n",
                            "name": "square",
                        }
                    ],
                },
                {
                    "sourceCode": "def cube(x: int) -> int:\n    return x ** 3\n",
                    "name": "cube",
                },
            ],
        },
        "featureSetName": "UserInfoSingleExtractor",
    }

    expected_extractor = ParseDict(f, fs_proto.Extractor())
    assert extractor_proto == expected_extractor, error_message(
        extractor_proto, expected_extractor
    )


@pytest.mark.integration
@mock_client
def test_simple_extractor(client):
    client.sync(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoSingleExtractor],
    )
    now = datetime.utcnow()
    data = [
        [18232, "John", 32, "USA", now],
        [18234, "Monica", 24, "Chile", now],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    response = client.log("UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    if client.is_integration_client():
        time.sleep(5)
    feature_df = client.extract_features(
        output_feature_list=[UserInfoSingleExtractor],
        input_feature_list=[UserInfoSingleExtractor.userid],
        input_dataframe=pd.DataFrame(
            {"UserInfoSingleExtractor.userid": [18232, 18234]}
        ),
    )
    assert feature_df.shape == (2, 5)
    assert feature_df["UserInfoSingleExtractor.age"].tolist() == [32, 24]
    assert feature_df["UserInfoSingleExtractor.age_power_four"].tolist() == [
        1048576,
        331776,
    ]
    assert feature_df["UserInfoSingleExtractor.age_cubed"].tolist() == [
        32768,
        13824,
    ]
