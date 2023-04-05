from datetime import datetime
from datetime import timedelta

import pandas as pd
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional, Dict, List

import fennel.gen.featureset_pb2 as fs_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.gen.dataset_pb2 import CoreDataset
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
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
    assert len(sync_request.datasets) == 1
    d = {
        "name": "UserInfoDataset",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"int_type": {}},
                    }
                ],
            },
            "values": {
                "fields": [
                    {"name": "name", "dtype": {"string_type": {}}},
                    {"name": "gender", "dtype": {"string_type": {}}},
                    {"name": "dob", "dtype": {"string_type": {}}},
                    {"name": "age", "dtype": {"int_type": {}}},
                    {
                        "name": "account_creation_date",
                        "dtype": {"timestamp_type": {}},
                    },
                    {
                        "name": "country",
                        "dtype": {"optional_type": {"of": {"string_type": {}}}},
                    },
                ],
            },
            "timestamp": "timestamp",
        },
        "metadata": {
            "owner": "aditya@fennel.ai",
            "description": "test",
            "tags": ["test"],
            "deprecated": True,
        },
        "history": "63072000s",
        "retention": "63072000s",
        "fieldMetadata": {
            "user_id": {},
            "name": {},
            "gender": {},
            "dob": {
                "description": "Users date of birth",
            },
            "age": {},
            "account_creation_date": {},
            "country": {},
            "timestamp": {},
        },
        "pycode": {},
    }
    expected_sync_request = ParseDict(d, CoreDataset())
    sync_request.datasets[0].pycode.Clear()
    expected_sync_request.pycode.Clear()
    assert sync_request.datasets[0] == expected_sync_request, error_message(
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
    assert len(sync_request.datasets) == 1
    d = {
        "datasets": [
            {
                "name": "YextUserInfoDataset",
                "dsschema": {
                    "keys": {
                        "fields": [
                            {
                                "name": "user_id",
                                "dtype": {"int_type": {}},
                            }
                        ],
                    },
                    "values": {
                        "fields": [
                            {"name": "name", "dtype": {"string_type": {}}},
                            {"name": "gender", "dtype": {"string_type": {}}},
                            {"name": "dob", "dtype": {"string_type": {}}},
                            {"name": "age", "dtype": {"int_type": {}}},
                            {
                                "name": "account_creation_date",
                                "dtype": {"timestamp_type": {}},
                            },
                            {
                                "name": "country",
                                "dtype": {
                                    "optional_type": {
                                        "of": {
                                            "map_type": {
                                                "key": {"string_type": {}},
                                                "value": {
                                                    "array_type": {
                                                        "of": {
                                                            "map_type": {
                                                                "key": {
                                                                    "string_type": {}
                                                                },
                                                                "value": {
                                                                    "double_type": {}
                                                                },
                                                            }
                                                        }
                                                    }
                                                },
                                            }
                                        }
                                    }
                                },
                            },
                        ],
                    },
                    "timestamp": "timestamp",
                },
                "metadata": {
                    "owner": "daniel@yext.com",
                    "description": "test",
                },
                "history": "31536000s",
                "retention": "31536000s",
                "fieldMetadata": {
                    "user_id": {
                        "description": "test",
                        "owner": "jack@yext.com",
                    },
                    "name": {},
                    "age": {},
                    "account_creation_date": {},
                    "country": {},
                    "gender": {"description": "sex", "tags": ["senstive"]},
                    "dob": {
                        "description": "Users date of birth",
                    },
                    "timestamp": {},
                },
            }
        ],
    }
    expected_sync_request = ParseDict(d, SyncRequest())
    expected_sync_request.datasets[0].pycode.Clear()
    sync_request.datasets[0].pycode.Clear()
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
    assert len(sync_request.feature_sets) == 1
    featureset_request = sync_request.feature_sets[0]
    f = {
        "name": "UserInfoSimple",
        "metadata": {
            "owner": "aditya@fennel.ai",
            "description": "test",
            "tags": ["test"],
        },
        "pycode": {},
    }
    expected_fs_request = ParseDict(f, fs_proto.CoreFeatureset())
    expected_fs_request.ClearField("pycode")
    featureset_request.ClearField("pycode")
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )

    assert len(sync_request.features) == 5
    actual_feature = sync_request.features[0]
    f = {
        "id": 1,
        "name": "userid",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfoSimple",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[1]
    f = {
        "id": 2,
        "name": "home_geoid",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfoSimple",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[2]
    f = {
        "id": 3,
        "name": "gender",
        "dtype": {"string_type": {}},
        "metadata": {"description": "The users gender among male/female"},
        "feature_set_name": "UserInfoSimple",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[3]
    f = {
        "id": 4,
        "name": "age_no_bar",
        "dtype": {"int_type": {}},
        "metadata": {"owner": "srk@bollywood.com"},
        "feature_set_name": "UserInfoSimple",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[4]
    f = {
        "id": 5,
        "name": "income",
        "dtype": {"int_type": {}},
        "metadata": {"deprecated": True},
        "feature_set_name": "UserInfoSimple",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
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

        @extractor(depends_on=[UserInfoDataset])
        @meta(owner="a@xyz.com", description="top_meta")
        @inputs(User.id)
        @outputs(userid, home_geoid)
        def get_user_info1(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor(depends_on=[UserInfoDataset])
        @inputs(User.id)
        @outputs(gender, age)
        @meta(owner="b@xyz.com", description="middle_meta")
        def get_user_info2(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor(depends_on=[UserInfoDataset])
        @inputs(User.id)
        @outputs(income)
        @meta(owner="c@xyz.com", description="bottom_meta")
        def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
            pass

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(User)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    assert len(sync_request.feature_sets) == 2
    featureset_request = sync_request.feature_sets[0]
    f = {
        "name": "UserInfo",
        "metadata": {"owner": "yolo@liveonce.com"},
    }
    expected_fs_request = ParseDict(f, fs_proto.CoreFeatureset())
    expected_fs_request.ClearField("pycode")
    featureset_request.ClearField("pycode")
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )

    assert len(sync_request.features) == 7
    # we will skip asserting the features, done above

    assert len(sync_request.extractors) == 3
    actual_extractor = erase_extractor_pycode(sync_request.extractors[0])
    e = {
        "name": "get_user_info1",
        "datasets": ["UserInfoDataset"],
        "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
        "features": ["userid", "home_geoid"],
        "metadata": {
            "owner": "a@xyz.com",
            "description": "top_meta",
        },
        "version": 0,
        "pycode": {
            "source_code": "",
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    expected_extractor.pycode.Clear()
    actual_extractor.pycode.Clear()
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )

    actual_extractor = erase_extractor_pycode(sync_request.extractors[1])
    e = {
        "name": "get_user_info2",
        "datasets": ["UserInfoDataset"],
        "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
        "features": ["gender", "age"],
        "metadata": {
            "owner": "b@xyz.com",
            "description": "middle_meta",
        },
        "version": 0,
        "pycode": {
            "source_code": "",
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )

    actual_extractor = erase_extractor_pycode(sync_request.extractors[2])
    e = {
        "name": "get_user_info3",
        "datasets": ["UserInfoDataset"],
        "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
        "features": ["income"],
        "metadata": {
            "owner": "c@xyz.com",
            "description": "bottom_meta",
        },
        "version": 0,
        "pycode": {
            "source_code": "",
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )
