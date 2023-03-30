from datetime import datetime
from typing import Optional

import pandas as pd
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as fs_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
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


def test_simple_featureset(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4).meta(owner="aditya@fennel.ai")
        income: int = feature(id=5).meta(deprecated=True)

        @extractor(depends_on=[UserInfoDataset], version=2)
        @inputs(User.id, User.age)
        def get_user_info(
            cls, ts: pd.Series, user_id: pd.Series, user_age: pd.Series
        ):
            return UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 1
    assert len(sync_request.extractors) == 1
    assert len(sync_request.features) == 5
    featureset_request = sync_request.feature_sets[0]
    f = {
        "name": "UserInfo",
        "metadata": {"owner": "test@test.com"},
    }
    expected_fs_request = ParseDict(f, fs_proto.CoreFeatureset())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )

    # features
    actual_feature = sync_request.features[0]
    f = {
        "id": 1,
        "name": "userid",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
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
        "feature_set_name": "UserInfo",
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
        "metadata": {
            "description": "The users gender among male/female/non-binary"
        },
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[3]
    f = {
        "id": 4,
        "name": "age",
        "dtype": {"int_type": {}},
        "metadata": {"owner": "aditya@fennel.ai"},
        "feature_set_name": "UserInfo",
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
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )

    # extractors
    actual_extractor = erase_extractor_pycode(sync_request.extractors[0])
    e = {
        "name": "get_user_info",
        "datasets": ["UserInfoDataset"],
        "inputs": [
            {"feature": {"feature_set_name": "User", "name": "id"}},
            {"feature": {"feature_set_name": "User", "name": "age"}},
        ],
        "features": [
            "userid",
            "home_geoid",
            "gender",
            "age",
            "income",
        ],
        "metadata": {},
        "version": 2,
        "pycode": {
            "source_code": "",
            "pickled": b"",
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )


def test_complex_featureset(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        userid: int = feature(id=1)
        home_geoid: int = feature(id=2)
        # The users gender among male/female/non-binary
        gender: str = feature(id=3)
        age: int = feature(id=4).meta(owner="aditya@fennel.ai")
        income: int = feature(id=5)

        @extractor(depends_on=[UserInfoDataset])
        @inputs(User.id)
        @outputs(userid, home_geoid)
        def get_user_info1(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor(depends_on=[UserInfoDataset])
        @inputs(User.id)
        @outputs(gender, age)
        def get_user_info2(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor
        @inputs(User.id)
        @outputs(income)
        def get_user_info3(cls, ts: pd.Series, user_id: pd.Series) -> income:
            pass

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 1
    assert len(sync_request.extractors) == 3
    assert len(sync_request.features) == 5
    f = {
        "name": "UserInfo",
        "metadata": {"owner": "test@test.com"},
    }
    featureset_request = sync_request.feature_sets[0]
    expected_fs_request = ParseDict(f, fs_proto.CoreFeatureset())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )

    # features
    actual_feature = sync_request.features[0]
    f = {
        "id": 1,
        "name": "userid",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
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
        "feature_set_name": "UserInfo",
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
        "metadata": {
            "description": "The users gender among male/female/non-binary"
        },
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[3]
    f = {
        "id": 4,
        "name": "age",
        "dtype": {"int_type": {}},
        "metadata": {"owner": "aditya@fennel.ai"},
        "feature_set_name": "UserInfo",
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
        "metadata": {},
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )

    # extractors
    actual_extractor = erase_extractor_pycode(sync_request.extractors[0])
    e = {
        "name": "get_user_info1",
        "datasets": ["UserInfoDataset"],
        "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
        "features": ["userid", "home_geoid"],
        "metadata": {},
        "version": 0,
        "pycode": {
            "source_code": "",
            "pickled": b"",
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )

    actual_extractor = erase_extractor_pycode(sync_request.extractors[1])
    e = {
        "name": "get_user_info2",
        "datasets": ["UserInfoDataset"],
        "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
        "features": ["gender", "age"],
        "metadata": {},
        "version": 0,
        "pycode": {
            "source_code": "",
            "pickled": b"",
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
        "datasets": [],
        "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
        "features": ["income"],
        "metadata": {},
        "version": 0,
        "pycode": {
            "source_code": "",
            "pickled": b"",
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )
