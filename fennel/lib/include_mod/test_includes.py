import time
from datetime import datetime
from typing import Optional

import pandas as pd
import pytest
import requests
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as fs_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.include_mod import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.test_lib import *


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def const() -> int:
    return 2


@includes(const)
def square(x: int) -> int:
    return x ** const()


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

    @extractor(depends_on=[UserInfoDataset])
    @includes(power_4, cube)
    @inputs(userid)
    @outputs(age, age_power_four, age_cubed, is_name_common)
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
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
            "entryPoint": "UserInfoSingleExtractor.get_user_info",
            "sourceCode": '@extractor(depends_on=[UserInfoDataset])\n@includes(power_4, cube)\n@inputs(userid)\n@outputs(age, age_power_four, age_cubed, is_name_common)\ndef get_user_info(cls, ts: pd.Series, user_id: pd.Series):\n    df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore\n    df[str(cls.userid)] = user_id\n    df[str(cls.age_power_four)] = power_4(df["age"])\n    df[str(cls.age_cubed)] = cube(df["age"])\n    df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])\n    return df[\n        [\n            str(cls.age),\n            str(cls.age_power_four),\n            str(cls.age_cubed),\n            str(cls.is_name_common),\n        ]\n    ]\n',
            "coreCode": '@extractor(depends_on=[UserInfoDataset])\n@includes(power_4, cube)\n@inputs(userid)\n@outputs(age, age_power_four, age_cubed, is_name_common)\ndef get_user_info(cls, ts: pd.Series, user_id: pd.Series):\n    df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore\n    df[str(cls.userid)] = user_id\n    df[str(cls.age_power_four)] = power_4(df["age"])\n    df[str(cls.age_cubed)] = cube(df["age"])\n    df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])\n    return df[\n        [\n            str(cls.age),\n            str(cls.age_power_four),\n            str(cls.age_cubed),\n            str(cls.is_name_common),\n        ]\n    ]\n',
            "generatedCode": '\nfrom datetime import datetime\nimport pandas as pd\nimport numpy as np\nimport functools\nfrom typing import List, Dict, Tuple, Optional, Union, Any, no_type_check\nfrom fennel.lib.metadata import meta\nfrom fennel.lib.include_mod import includes\nfrom fennel.datasets import *\nfrom fennel.featuresets import *\nfrom fennel.lib.schema import *\nfrom fennel.datasets.datasets import dataset_lookup\n\ndef const() -> int:\n    return 2\n\n@includes(const)\ndef square(x: int) -> int:\n    return x ** const()\n\n@includes(square)\ndef power_4(x: int) -> int:\n    return square(square(x))\n\ndef cube(x: int) -> int:\n    return x ** 3\n\n\n@meta(owner="test@test.com")\n@dataset\nclass UserInfoDataset:\n    user_id: int = field(key=True)\n    name: str\n    age: Optional[int]\n    timestamp: datetime = field(timestamp=True)\n    country: str\n\n@meta(owner="test@test.com")\n@featureset\nclass UserInfoSingleExtractor:\n    userid: int = feature(id=1)\n    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore\n    age_power_four: int = feature(id=5)\n    age_cubed: int = feature(id=6)\n    is_name_common: bool = feature(id=7)\n\n\n\n@meta(owner="test@test.com")\n@featureset\nclass UserInfoSingleExtractor:\n    userid: int = feature(id=1)\n    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore\n    age_power_four: int = feature(id=5)\n    age_cubed: int = feature(id=6)\n    is_name_common: bool = feature(id=7)\n\n    @extractor(depends_on=[UserInfoDataset])\n    @includes(power_4, cube)\n    @inputs(userid)\n    @outputs(age, age_power_four, age_cubed, is_name_common)\n    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):\n        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore\n        df[str(cls.userid)] = user_id\n        df[str(cls.age_power_four)] = power_4(df["age"])\n        df[str(cls.age_cubed)] = cube(df["age"])\n        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])\n        return df[\n            [\n                str(cls.age),\n                str(cls.age_power_four),\n                str(cls.age_cubed),\n                str(cls.is_name_common),\n            ]\n        ]\n',
            "includes": [
                {
                    "entryPoint": "power_4",
                    "sourceCode": "@includes(square)\ndef power_4(x: int) -> int:\n    return square(square(x))\n",
                    "coreCode": "@includes(square)\ndef power_4(x: int) -> int:\n    return square(square(x))\n",
                    "generatedCode": "@includes(square)\ndef power_4(x: int) -> int:\n    return square(square(x))\n",
                    "includes": [
                        {
                            "entryPoint": "square",
                            "sourceCode": "@includes(const)\ndef square(x: int) -> int:\n    return x ** const()\n",
                            "coreCode": "@includes(const)\ndef square(x: int) -> int:\n    return x ** const()\n",
                            "generatedCode": "@includes(const)\ndef square(x: int) -> int:\n    return x ** const()\n",
                            "includes": [
                                {
                                    "entryPoint": "const",
                                    "sourceCode": "def const() -> int:\n    return 2\n",
                                    "coreCode": "def const() -> int:\n    return 2\n",
                                    "generatedCode": "def const() -> int:\n    return 2\n",
                                }
                            ],
                        }
                    ],
                },
                {
                    "entryPoint": "cube",
                    "sourceCode": "def cube(x: int) -> int:\n    return x ** 3\n",
                    "coreCode": "def cube(x: int) -> int:\n    return x ** 3\n",
                    "generatedCode": "def cube(x: int) -> int:\n    return x ** 3\n",
                },
            ],
            "refIncludes": {
                "UserInfoSingleExtractor": "Featureset",
                "UserInfoDataset": "Dataset",
            },
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
