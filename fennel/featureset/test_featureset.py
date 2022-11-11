from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
from google.protobuf.json_format import ParseDict

import fennel.gen.featureset_pb2 as proto
from fennel.dataset import dataset, field
from fennel.featureset import featureset, extractor, depends_on, feature
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
    id: int = feature(id=1)
    age: float = feature(id=2)


def test_SimpleFeatureSet(grpc_stub):
    @featureset
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2, wip=True)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4, owner="aditya@fennel.ai")
        income: int = feature(id=5, deprecated=True)

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info(
            ts: pd.Series, user: User, user_id: User.id, user_age: User.age
        ):
            return UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore

    @featureset
    class UserInfoDuplicate:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2, wip=True)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4, owner="aditya@fennel.ai")
        income: int = feature(id=5, deprecated=True)

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info(
            ts: pd.Series, user: User, user_id: User.id, user_age: User.age
        ):
            return UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(UserInfoDuplicate)
    sync_request = view.to_proto()
    assert len(sync_request.featureset_requests) == 2
    featureset_request = clean_fs_func_src_code(
        sync_request.featureset_requests[0]
    )

    # Both requests should be the same, apart from the name.
    sync_request.featureset_requests[1].name = "UserInfo"
    assert featureset_request == clean_fs_func_src_code(
        sync_request.featureset_requests[1]
    )

    f = {
        "name": "UserInfo",
        "features": [
            {"id": 1, "name": "userid", "dtype": "int64"},
            {"id": 2, "name": "home_geoid", "dtype": "int64", "wip": True},
            {
                "id": 3,
                "name": "gender",
                "dtype": "string",
                "description": "The users gender among male/female/non-binary",
            },
            {
                "id": 4,
                "name": "age",
                "dtype": "int64",
                "owner": "aditya@fennel.ai",
            },
            {"id": 5, "name": "income", "dtype": "int64", "deprecated": True},
        ],
        "extractors": [
            {
                "name": "get_user_info",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"featureSet": {"name": "User"}},
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}},
                    {
                        "feature": {
                            "featureSet": {"name": "User"},
                            "name": "age",
                        }
                    },
                ],
            }
        ],
    }

    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )


def test_ComplexFeatureSet(grpc_stub):
    @featureset
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4, owner="aditya@fennel.ai")
        income: int = feature(id=5)

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info1(
            ts: pd.Series, user_id: User.id
        ) -> Tuple["userid", "home_geoid"]:
            pass

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info2(
            ts: pd.Series, user_id: User.id
        ) -> Tuple["gender", "age"]:
            pass

        @extractor
        def get_user_info3(ts: pd.Series, user_id: User.id) -> Tuple["income"]:
            pass

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    sync_request = view.to_proto()
    assert len(sync_request.featureset_requests) == 1
    featureset_request = clean_fs_func_src_code(
        sync_request.featureset_requests[0]
    )
    f = {
        "name": "UserInfo",
        "features": [
            {"id": 1, "name": "userid", "dtype": "int64"},
            {"id": 2, "name": "home_geoid", "dtype": "int64"},
            {
                "id": 3,
                "name": "gender",
                "dtype": "string",
                "description": "The users gender among male/female/non-binary",
            },
            {
                "id": 4,
                "name": "age",
                "dtype": "int64",
                "owner": "aditya@fennel.ai",
            },
            {"id": 5, "name": "income", "dtype": "int64"},
        ],
        "extractors": [
            {
                "name": "get_user_info1",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["userid", "home_geoid"],
            },
            {
                "name": "get_user_info2",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["gender", "age"],
            },
            {
                "name": "get_user_info3",
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["income"],
            },
        ],
    }
    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())

    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )
