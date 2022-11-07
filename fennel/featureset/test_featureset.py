from datetime import datetime
from typing import Optional

import pandas as pd
from google.protobuf.json_format import ParseDict

import fennel.gen.featureset_pb2 as proto
from fennel.dataset import dataset
from fennel.featureset import featureset, extractor, depends_on
from fennel.lib.field import field
from fennel.test_lib import *


@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    gender: str
    # Users date of birth
    dob: str
    age: int
    account_creation_date: datetime
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


@featureset
class User:
    id: int
    age: float


def test_SimpleFeatureSet(grpc_stub):
    @featureset
    class UserInfo:
        userid: int
        home_geoid: int
        # The users gender among male/female/non-binary
        gender: str
        age: int = field(owner="aditya@fennel.ai")
        income: int

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info(ts: pd.Series, user: User, user_id: User.id, user_age:
        User.age) -> "UserInfo":
            return UserInfoDataset.lookup(ts, user_id=user_id)

    view = InternalTestView(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    sync_request = view.to_proto()
    assert len(sync_request.featureset_requests) == 1
    featureset_request = clean_fs_func_src_code(
        sync_request.featureset_requests[0])
    f = {
        "name": "UserInfo",
        "features": [
            {
                "name": "userid",
                "dtype": "int64"
            },
            {
                "name": "home_geoid",
                "dtype": "int64"
            },
            {
                "name": "gender",
                "dtype": "string"
            },
            {
                "name": "age",
                "dtype": "int64",
                "owner": "aditya@fennel.ai"
            },
            {
                "name": "income",
                "dtype": "int64"
            }
        ],
        "extractors": [
            {
                "name": "get_user_info",
                "datasets": [
                    "UserInfoDataset"
                ],
                "inputs": [
                    {
                        "featureSet": {
                            "name": "User"
                        }
                    },
                    {
                        "feature": {
                            "featureSet": {
                                "name": "User"
                            },
                            "name": "id"
                        }
                    },
                    {
                        "feature": {
                            "featureSet": {
                                "name": "User"
                            },
                            "name": "age"
                        }
                    }
                ]
            }
        ]
    }

    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request)
