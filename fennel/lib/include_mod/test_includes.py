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
    return x ** 2


def cube(x: int) -> int:
    return x ** 3


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
        'name': 'get_user_info',
        'datasets': [
            'UserInfoDataset'
        ],
        'inputs': [
            {
                'feature': {
                    'featureSetName': 'UserInfoSingleExtractor',
                    'name': 'userid'
                }
            }
        ],
        'features': [
            'age',
            'age_power_four',
            'age_cubed',
            'is_name_common'
        ],
        'metadata': {

        },
        'pycode': {
            'sourceCode': '\n\n\ndef get_user_info(\n        cls, ts: Series[datetime], user_id: Series[userid]\n) -> DataFrame[age, age_power_four, age_cubed, is_name_common]:\n    df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore\n    df[str(cls.userid)] = user_id\n    df[str(cls.age_power_four)] = power_4(df["age"])\n    df[str(cls.age_cubed)] = cube(df["age"])\n    df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])\n    return df[\n        [\n            str(cls.age),\n            str(cls.age_power_four),\n            str(cls.age_cubed),\n            str(cls.is_name_common),\n        ]\n    ]\n',
            'extractorName': 'get_user_info',
            'includes': [
                {
                    'sourceCode': '\ndef power_4(x: int) -> int:\n    return square(square(x))\n',
                    'name': 'power_4',
                    'includes': [
                        {
                            'sourceCode': 'def square(x: int) -> int:\n    return x ** 2\n',
                            'name': 'square'
                        }
                    ]
                },
                {
                    'sourceCode': 'def cube(x: int) -> int:\n    return x ** 3\n',
                    'name': 'cube'
                }
            ],
            'datasetCodes': [
                '\n\nclass UserInfoDataset:\n    user_id: int = field(key=True)\n    name: str\n    age: Optional[int]\n    timestamp: datetime = field(timestamp=True)\n    country: str\n    @classmethod\n    def lookup(\n        cls, ts: pd.Series, *args, **kwargs\n    ) -> Tuple[pd.DataFrame, pd.Series]:\n        if len(args) > 0:\n            raise ValueError(\n                f"lookup expects key value arguments and can "\n                f"optionally include fields, found {args}"\n            )\n        if len(kwargs) < len(cls.key_fields()):\n            raise ValueError(\n                f"lookup expects keys of the table being looked up and can "\n                f"optionally include fields, found {kwargs}"\n            )\n        # Check that ts is a series of datetime64[ns]\n        if not isinstance(ts, pd.Series):\n            raise ValueError(\n                f"lookup expects a series of timestamps, found {type(ts)}"\n            )\n        if not np.issubdtype(ts.dtype, np.datetime64):\n            raise ValueError(\n                f"lookup expects a series of timestamps, found {ts.dtype}"\n            )\n        # extract keys and fields from kwargs\n        arr = []\n        for key in cls.key_fields():\n            if key == "fields":\n                continue\n            if key not in kwargs:\n                raise ValueError(\n                    f"Missing key {key} in the lookup call "\n                    f"for dataset `{cls.__name__}`"\n                )\n            if not isinstance(kwargs[key], pd.Series):\n                raise ValueError(\n                    f"Param `{key}` is not a pandas Series "\n                    f"in the lookup call for dataset `{cls.__name__}`"\n                )\n            arr.append(kwargs[key])\n\n        if "fields" in kwargs:\n            fields = kwargs["fields"]\n        else:\n            fields = []\n\n        df = pd.concat(arr, axis=1)\n        df.columns = cls.key_fields()\n        res, found = dataset_lookup(\n            cls.__name__,\n            ts,\n            fields,\n            df,\n        )\n\n        return res.replace({np.nan: None}), found\n\n    @classmethod\n    def key_fields(cls) -> List[str]:\n        return [\'user_id\']\n'
            ],
            'datasetNames': [
                'UserInfoDataset'
            ],
            'featuresetCode': '\n\nclass UserInfoSingleExtractor:\n    userid: int = feature(id=1)\n    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore\n    age_power_four: int = feature(id=5)\n    age_cubed: int = feature(id=6)\n    is_name_common: bool = feature(id=7)\n\n\n\n    def get_user_info(\n            cls, ts: Series[datetime], user_id: Series[userid]\n    ) -> DataFrame[age, age_power_four, age_cubed, is_name_common]:\n        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore\n        df[str(cls.userid)] = user_id\n        df[str(cls.age_power_four)] = power_4(df["age"])\n        df[str(cls.age_cubed)] = cube(df["age"])\n        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])\n        return df[\n            [\n                str(cls.age),\n                str(cls.age_power_four),\n                str(cls.age_cubed),\n                str(cls.is_name_common),\n            ]\n        ]\n\n    @classproperty\n    def userid(cls):\n        return "userid"\n\n\n\n    @classproperty\n    def age(cls):\n        return "age"\n\n\n\n    @classproperty\n    def age_power_four(cls):\n        return "age_power_four"\n\n\n\n    @classproperty\n    def age_cubed(cls):\n        return "age_cubed"\n\n\n\n    @classproperty\n    def is_name_common(cls):\n        return "is_name_common"\n\n\n',
            'imports': '\nfrom datetime import datetime\nimport pandas as pd\nimport numpy as np\nfrom typing import List, Dict, Tuple, Optional, Union, Any\nfrom fennel.featuresets import *\nfrom fennel.lib.metadata import meta\nfrom fennel.lib.include_mod import includes\nfrom fennel.datasets import *\nfrom fennel.lib.schema import *\nfrom fennel.datasets.datasets import dataset_lookup\nclass classproperty(object):\n    def __init__(self, f):\n        self.f = classmethod(f)\n    def __get__(self, *a):\n        return self.f.__get__(*a)()\n'
        },
        'featureSetName': 'UserInfoSingleExtractor'
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
