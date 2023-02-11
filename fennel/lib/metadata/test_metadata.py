from datetime import datetime
from datetime import timedelta
from typing import Optional, Dict, List

from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as proto
import fennel.gen.services_pb2 as service_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.metadata import meta
from fennel.lib.schema import DataFrame, Series
from fennel.test_lib import *


@meta(
    owner="aditya@fennel.ai",
    description="test",
    tags=["test"],
    deprecated=True,
)
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


def test_simple_dataset(grpc_stub):
    assert UserInfoDataset._history == timedelta(days=730)
    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "UserInfoDataset",
                "fields": [
                    {
                        "name": "user_id",
                        "ftype": "Key",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "name",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "gender",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "dob",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {"description": "Users date of birth"},
                    },
                    {
                        "name": "age",
                        "ftype": "Val",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "account_creation_date",
                        "ftype": "Val",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                    {
                        "name": "country",
                        "ftype": "Val",
                        "dtype": {"isNullable": True, "scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "timestamp",
                        "ftype": "Timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                ],
                "signature": "88468f81bc9e7be9c988fb90d3299df9",
                "metadata": {
                    "owner": "aditya@fennel.ai",
                    "description": "test",
                    "tags": ["test"],
                    "deprecated": True,
                },
                "mode": "pandas",
                "history": "63072000000000",
            }
        ]
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_complex_dataset_with_fields(grpc_stub):
    @dataset(history="1y")
    @meta(owner="daniel@yext.com", description="test")
    class YextUserInfoDataset:
        user_id: int = field(key=True).meta(
            description="test", owner="jack@yext.com"
        )
        name: str
        gender: str = field().meta(description="sex", tags=["senstive"])
        # Users date of birth
        dob: str
        age: int = field()
        account_creation_date: datetime
        country: Optional[Dict[str, List[Dict[str, float]]]] = field()
        timestamp: datetime = field(timestamp=True)

    assert YextUserInfoDataset._history == timedelta(days=365)
    view = InternalTestClient(grpc_stub)
    view.add(YextUserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "YextUserInfoDataset",
                "fields": [
                    {
                        "name": "user_id",
                        "ftype": "Key",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {
                            "owner": "jack@yext.com",
                            "description": "test",
                        },
                    },
                    {
                        "name": "name",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "gender",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {
                            "description": "sex",
                            "tags": ["senstive"],
                        },
                    },
                    {
                        "name": "dob",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {"description": "Users date of birth"},
                    },
                    {
                        "name": "age",
                        "ftype": "Val",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "account_creation_date",
                        "ftype": "Val",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                    {
                        "name": "country",
                        "ftype": "Val",
                        "dtype": {
                            "isNullable": True,
                            "mapType": {
                                "key": {"scalarType": "STRING"},
                                "value": {
                                    "arrayType": {
                                        "of": {
                                            "mapType": {
                                                "key": {"scalarType": "STRING"},
                                                "value": {
                                                    "scalarType": "FLOAT"
                                                },
                                            }
                                        }
                                    }
                                },
                            },
                        },
                        "metadata": {},
                    },
                    {
                        "name": "timestamp",
                        "ftype": "Timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                ],
                "signature": "020a32d8f46e7c5f6cffb096205c0a1c",
                "metadata": {
                    "owner": "daniel@yext.com",
                    "description": "test",
                },
                "mode": "pandas",
                "history": "31536000000000",
            }
        ]
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_simple_featureset(grpc_stub):
    @meta(owner="aditya@fennel.ai", description="test", tags=["test"])
    @featureset
    class UserInfoSimple:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2)
        # The users gender among male/female
        gender: str = feature(id=3)
        age_no_bar: int = feature(id=4).meta(owner="srk@bollywood.com")
        income: int = feature(id=5).meta(deprecated=True)

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoSimple)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.featureset_requests) == 1
    featureset_request = clean_fs_func_src_code(
        sync_request.featureset_requests[0]
    )
    f = {
        "name": "UserInfoSimple",
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
                    "description": "The users gender among male/female"
                },
            },
            {
                "id": 4,
                "name": "age_no_bar",
                "dtype": {"scalarType": "INT"},
                "metadata": {"owner": "srk@bollywood.com"},
            },
            {
                "id": 5,
                "name": "income",
                "dtype": {"scalarType": "INT"},
                "metadata": {"deprecated": True},
            },
        ],
        "metadata": {
            "owner": "aditya@fennel.ai",
            "description": "test",
            "tags": ["test"],
        },
    }
    expected_fs_request = ParseDict(f, service_proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )


def test_featureset_with_extractors(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class User:
        id: int = feature(id=1)
        age: float = feature(id=2)

    @featureset
    @meta(owner="yolo@liveonce.com")
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2)
        # The users gender among male/female
        gender: str = feature(id=3)
        age: int = feature(id=4).meta(owner="aditya@fennel.ai")
        income: int = feature(id=5)

        @meta(owner="a@xyz.com", description="top_meta")
        @extractor
        @depends_on(UserInfoDataset)
        def get_user_info1(
            cls, ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[userid, home_geoid]:
            pass

        @extractor
        @meta(owner="b@xyz.com", description="middle_meta")
        @depends_on(UserInfoDataset)
        def get_user_info2(
            cls, ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[gender, age]:
            pass

        @extractor
        @depends_on(UserInfoDataset)
        @meta(owner="c@xyz.com", description="bottom_meta")
        def get_user_info3(
            cls, ts: Series[datetime], user_id: Series[User.id]
        ) -> Series[income]:
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
                    "description": "The users gender among male/female"
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
                "metadata": {"owner": "a@xyz.com", "description": "top_meta"},
            },
            {
                "name": "UserInfo.get_user_info2",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["gender", "age"],
                "metadata": {
                    "owner": "b@xyz.com",
                    "description": "middle_meta",
                },
            },
            {
                "name": "UserInfo.get_user_info3",
                "datasets": ["UserInfoDataset"],
                "inputs": [
                    {"feature": {"featureSet": {"name": "User"}, "name": "id"}}
                ],
                "features": ["income"],
                "metadata": {
                    "owner": "c@xyz.com",
                    "description": "bottom_meta",
                },
            },
        ],
        "metadata": {"owner": "yolo@liveonce.com"},
    }
    expected_fs_request = ParseDict(f, service_proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )
