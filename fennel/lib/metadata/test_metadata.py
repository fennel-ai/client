from datetime import datetime
from datetime import timedelta
from typing import Optional, Dict, List

from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as proto
from fennel.datasets import dataset, pipeline, field, Dataset
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


def test_simpleDataset(grpc_stub):
    assert UserInfoDataset._retention == timedelta(days=730)
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
                "signature": "b7cb8565c45b59f577d655496226cdae",
                "metadata": {
                    "owner": "aditya@fennel.ai",
                    "description": "test",
                    "tags": ["test"],
                    "deprecated": True,
                },
                "mode": "pandas",
                "retention": "63072000000000",
            }
        ]
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_complexDatasetWithFields(grpc_stub):
    @dataset(retention="1y")
    @meta(owner="daniel@yext.com", description="test", wip=True)
    class YextUserInfoDataset:
        user_id: int = field(key=True).meta(
            description="test", owner="jack@yext.com"
        )
        name: str
        gender: str = field().meta(description="sex", tags=["senstive"])
        # Users date of birth
        dob: str
        age: int = field().meta(wip=True)
        account_creation_date: datetime
        country: Optional[Dict[str, List[Dict[str, float]]]] = field()
        timestamp: datetime = field(timestamp=True)

    assert YextUserInfoDataset._retention == timedelta(days=365)
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
                        "metadata": {"wip": True},
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
                "signature": "203421af01d980b5bc20e73454eb4d1b",
                "metadata": {
                    "owner": "daniel@yext.com",
                    "description": "test",
                    "wip": True,
                },
                "mode": "pandas",
                "retention": "31536000000000",
            }
        ]
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_DatasetWithPipes(grpc_stub):
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @dataset
    class B:
        b1: int = field(key=True)
        t: datetime

    @dataset
    class C:
        t: datetime

    @dataset
    @meta(
        owner="aditya@fennel.ai",
        description="test",
    )
    class ABCDataset:
        a: int = field(key=True)
        b: int = field(key=True).meta(description="test")
        c: int
        d: datetime

        @staticmethod
        @meta(owner="a@xyz.com", description="top_meta")
        @pipeline(A, B, C)
        def pipeline2(a: Dataset, b: Dataset, c: Dataset):
            return c

        @staticmethod
        @pipeline(A, B, C)
        @meta(owner="b@xyz.com", description="bottom_meta")
        def pipeline3(a: Dataset, b: Dataset, c: Dataset):
            return c

    view = InternalTestClient(grpc_stub)
    view.add(ABCDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "ABCDataset",
                "fields": [
                    {
                        "name": "a",
                        "ftype": "Key",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "b",
                        "ftype": "Key",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {"description": "test"},
                    },
                    {
                        "name": "c",
                        "ftype": "Val",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "d",
                        "ftype": "Timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                ],
                "pipelines": [
                    {
                        "root": "C",
                        "signature": "ABCDataset.C",
                        "metadata": {
                            "owner": "a@xyz.com",
                            "description": "top_meta",
                        },
                        "inputs": ["A", "B", "C"],
                    },
                    {
                        "root": "C",
                        "signature": "ABCDataset.C",
                        "metadata": {
                            "owner": "b@xyz.com",
                            "description": "bottom_meta",
                        },
                        "inputs": ["A", "B", "C"],
                    },
                ],
                "signature": "90c6d47cba9c621df5221fe1126ee606",
                "metadata": {
                    "owner": "aditya@fennel.ai",
                    "description": "test",
                },
                "mode": "pandas",
                "retention": "63072000000000",
            }
        ]
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_simpleFeatureSet(grpc_stub):
    @meta(owner="aditya@fennel.ai", description="test", tags=["test"])
    @featureset
    class UserInfoSimple:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2).meta(wip=True)
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
                "metadata": {"wip": True},
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
    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )


def test_featuresetWithExtractors(grpc_stub):
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
            ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[userid, home_geoid]:
            pass

        @extractor
        @meta(owner="b@xyz.com", description="middle_meta")
        @depends_on(UserInfoDataset)
        def get_user_info2(
            ts: Series[datetime], user_id: Series[User.id]
        ) -> DataFrame[gender, age]:
            pass

        @extractor
        @depends_on(UserInfoDataset)
        @meta(owner="c@xyz.com", description="bottom_meta")
        def get_user_info3(
            ts: Series[datetime], user_id: Series[User.id]
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
    expected_fs_request = ParseDict(f, proto.CreateFeaturesetRequest())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )
