from datetime import datetime
from typing import Optional

from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import *


@meta(owner="test@test.com")
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


@meta(owner="test@test.com")
@featureset
class User:
    id: int = feature(id=1)
    age: float = feature(id=2)


def test_SimpleFeatureSet(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2).meta(wip=True)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4).meta(owner="aditya@fennel.ai")
        income: int = feature(id=5).meta(deprecated=True)

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info(
            ts: Series[datetime],
            user: DataFrame[User],
            user_id: Series[User.id],
            user_age: Series[User.age],
        ):
            return UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.featureset_requests) == 1
    featureset_request = clean_fs_func_src_code(
        sync_request.featureset_requests[0]
    )

    f = {
        "name": "UserInfo",
        "features": [
            {
                "id": 1,
                "name": "userid",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "id": 2,
                "name": "home_geoid",
                "dtype": {"scalarType": "INT"},
                "metadata": {"wip": True},
            },
            {
                "id": 3,
                "name": "gender",
                "dtype": {"scalarType": "STRING"},
                "metadata": {
                    "description": "The users gender among male/female/non-binary"
                },
            },
            {
                "id": 4,
                "name": "age",
                "dtype": {"scalarType": "INT"},
                "metadata": {"owner": "aditya@fennel.ai"},
            },
            {
                "id": 5,
                "name": "income",
                "dtype": {"scalarType": "INT"},
                "metadata": {"deprecated": True},
            },
        ],
        "extractors": [
            {
                "name": "UserInfo.get_user_info",
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
                "features": ["userid", "home_geoid", "gender", "age", "income"],
                "metadata": {},
            }
        ],
        "metadata": {"owner": "test@test.com"},
    }

    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )


def test_ComplexFeatureSet(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4).meta(owner="aditya@fennel.ai")
        income: int = feature(id=5)

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info1(
            ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[userid, home_geoid]:
            pass

        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info2(
            ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[gender, age]:
            pass

        @extractor
        def get_user_info3(
            ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[income]:
            pass

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.featureset_requests) == 1
    featureset_request = clean_fs_func_src_code(
        sync_request.featureset_requests[0]
    )
    f = {
        "name": "UserInfo",
        "features": [
            {
                "id": 1,
                "name": "userid",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "id": 2,
                "name": "home_geoid",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "id": 3,
                "name": "gender",
                "dtype": {"scalarType": "STRING"},
                "metadata": {
                    "description": "The users gender among male/female/non-binary"
                },
            },
            {
                "id": 4,
                "name": "age",
                "dtype": {"scalarType": "INT"},
                "metadata": {"owner": "aditya@fennel.ai"},
            },
            {
                "id": 5,
                "name": "income",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
        ],
        "extractors": [
            {
                "name": "UserInfo.get_user_info1",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["userid", "home_geoid"],
                "metadata": {},
            },
            {
                "name": "UserInfo.get_user_info2",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["gender", "age"],
                "metadata": {},
            },
            {
                "name": "UserInfo.get_user_info3",
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["income"],
                "metadata": {},
            },
        ],
        "metadata": {"owner": "test@test.com"},
    }
    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())

    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )
