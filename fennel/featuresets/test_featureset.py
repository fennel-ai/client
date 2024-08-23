from datetime import datetime
from typing import Optional

import pandas as pd
import pytest
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.featureset_pb2 as fs_proto
from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature as F
from fennel.lib import meta, inputs, outputs, desc
from fennel.testing import *
from fennel.expr import col

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
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
    avg_income: int


@meta(owner="test@test.com")
@featureset
class User:
    id: int
    age: float


def test_simple_featureset():
    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        userid: int
        # The users home geoid
        # This is a multiline comment
        home_geoid: int
        # The users gender among male/female/non-binary
        gender: str
        age: int = F().meta(owner="aditya@fennel.ai")
        age_sq: int = F(col("age") * col("age"))
        age_double: int = F(col("age") * 2)
        height: int
        weight: float
        bmi: float = F(col("weight") / col("height") / col("height") * 2.20462)
        income: int = F().meta(deprecated=True)

        @extractor(deps=[UserInfoDataset], version=2)
        @inputs(User.id, User.age)
        @outputs("userid", "home_geoid", "gender", "age", "income")
        def get_user_info(
            cls, ts: pd.Series, user_id: pd.Series, user_age: pd.Series
        ):
            return UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore

    assert (
        desc(UserInfo.gender) == "The users gender among male/female/non-binary"
    )
    assert (
        desc(UserInfo.home_geoid)
        == "The users home geoid\nThis is a multiline comment"
    )

    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(User)
    assert UserInfo.all() == [
        "UserInfo.userid",
        "UserInfo.home_geoid",
        "UserInfo.gender",
        "UserInfo.age",
        "UserInfo.age_sq",
        "UserInfo.age_double",
        "UserInfo.height",
        "UserInfo.weight",
        "UserInfo.bmi",
        "UserInfo.income",
    ]
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 2
    assert len(sync_request.extractors) == 4
    assert len(sync_request.features) == 12
    featureset_request = sync_request.feature_sets[0]
    f = {
        "name": "UserInfo",
        "metadata": {"owner": "test@test.com"},
        "pycode": {"source_code": ""},
    }
    expected_fs_request = ParseDict(f, fs_proto.CoreFeatureset())
    # Clear the pycode field in featureset_request as it is not deterministic
    featureset_request.pycode.Clear()

    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )

    # features
    actual_feature = sync_request.features[0]
    f = {
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
        "name": "home_geoid",
        "dtype": {"int_type": {}},
        "metadata": {
            "description": "The users home geoid\nThis is a multiline comment"
        },
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[2]
    f = {
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
        "name": "age_sq",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[5]
    f = {
        "name": "age_double",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[6]
    f = {
        "name": "height",
        "dtype": {"int_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[7]
    f = {
        "name": "weight",
        "dtype": {"double_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[8]
    f = {
        "name": "bmi",
        "dtype": {"double_type": {}},
        "metadata": {},
        "feature_set_name": "UserInfo",
    }
    expected_feature = ParseDict(f, fs_proto.Feature())
    assert actual_feature == expected_feature, error_message(
        actual_feature, expected_feature
    )
    actual_feature = sync_request.features[9]
    f = {
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
    actual_extractor = sync_request.extractors[0]
    e = {
        "name": "_fennel_expr_UserInfo.age_sq",
        "inputs": [
            {
                "feature": {"featureSetName": "UserInfo", "name": "age"},
                "dtype": {"intType": {}},
            }
        ],
        "features": ["age_sq"],
        "metadata": {},
        "featureSetName": "UserInfo",
        "extractorType": "EXPR",
        "expr": {
            "binary": {
                "left": {"ref": {"name": "age"}},
                "right": {"ref": {"name": "age"}},
                "op": "MUL",
            }
        },
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )

    actual_extractor = sync_request.extractors[1]
    e = {
        "name": "_fennel_expr_UserInfo.age_double",
        "inputs": [
            {
                "feature": {"featureSetName": "UserInfo", "name": "age"},
                "dtype": {"intType": {}},
            }
        ],
        "features": ["age_double"],
        "metadata": {},
        "featureSetName": "UserInfo",
        "extractorType": "EXPR",
        "expr": {
            "binary": {
                "left": {"ref": {"name": "age"}},
                "right": {
                    "jsonLiteral": {"literal": "2", "dtype": {"intType": {}}}
                },
                "op": "MUL",
            }
        },
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )

    actual_extractor = erase_extractor_pycode(sync_request.extractors[3])
    e = {
        "name": "get_user_info",
        "datasets": ["UserInfoDataset"],
        "inputs": [
            {
                "feature": {"feature_set_name": "User", "name": "id"},
                "dtype": {"int_type": {}},
            },
            {
                "feature": {"feature_set_name": "User", "name": "age"},
                "dtype": {"double_type": {}},
            },
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
        },
        "feature_set_name": "UserInfo",
    }
    expected_extractor = ParseDict(e, fs_proto.Extractor())
    assert actual_extractor == expected_extractor, error_message(
        actual_extractor, expected_extractor
    )


def test_complex_featureset():
    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        userid: int
        home_geoid: int
        # The users gender among male/female/non-binary
        gender: str
        age: int = F().meta(owner="aditya@fennel.ai")
        income: int

        @extractor(deps=[UserInfoDataset])
        @inputs(User.id)
        @outputs("userid", "home_geoid")
        def get_user_info1(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor(deps=[UserInfoDataset])
        @inputs(User.id)
        @outputs("gender", "age")
        def get_user_info2(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor
        @inputs(User.id)
        @outputs("income")
        def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
            pass

    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(User)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 2
    assert len(sync_request.extractors) == 3
    assert len(sync_request.features) == 7
    f = {
        "name": "UserInfo",
        "metadata": {"owner": "test@test.com"},
        "pycode": {},
    }
    featureset_request = sync_request.feature_sets[0]
    featureset_request.pycode.Clear()
    expected_fs_request = ParseDict(f, fs_proto.CoreFeatureset())
    assert featureset_request == expected_fs_request, error_message(
        featureset_request, expected_fs_request
    )

    # features
    actual_feature = sync_request.features[0]
    f = {
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
        "inputs": [
            {
                "feature": {"feature_set_name": "User", "name": "id"},
                "dtype": {"int_type": {}},
            }
        ],
        "features": ["userid", "home_geoid"],
        "metadata": {},
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

    actual_extractor = erase_extractor_pycode(sync_request.extractors[1])
    e = {
        "name": "get_user_info2",
        "datasets": ["UserInfoDataset"],
        "inputs": [
            {
                "feature": {"feature_set_name": "User", "name": "id"},
                "dtype": {"int_type": {}},
            }
        ],
        "features": ["gender", "age"],
        "metadata": {},
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
        "datasets": [],
        "inputs": [
            {
                "feature": {"feature_set_name": "User", "name": "id"},
                "dtype": {"int_type": {}},
            }
        ],
        "features": ["income"],
        "metadata": {},
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


def test_extractor_env_selector():
    @meta(owner="aditya@fennel.ai")
    @featureset
    class Request:
        user_id: int

    @meta(owner="aditya@fennel.ai")
    @featureset
    class UserInfo:
        user_id: int = F(Request.user_id, env=["~staging", "~prod"])
        home_geoid: int
        # The users gender among male/female/non-binary
        gender: str
        age: int = F().meta(owner="aditya@fennel.ai")
        income: int = F(
            UserInfoDataset.avg_income,
            default=1,
            env=["~prod"],
        )

        @extractor(deps=[UserInfoDataset], env=["~prod", "~dev"])
        @inputs(User.id)
        @outputs(user_id, "home_geoid")
        def get_user_info1(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor(deps=[UserInfoDataset], env=["prod"])
        @inputs(User.id)
        @outputs(user_id, "home_geoid")
        def get_user_info2(cls, ts: pd.Series, user_id: pd.Series):
            pass

        @extractor(deps=[UserInfoDataset], env=["prod"])
        @inputs(User.id)
        @outputs(income)
        def get_user_income(cls, ts: pd.Series, user_id: pd.Series):
            pass

    view = InternalTestClient()
    view.add(Request)
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(User)
    with pytest.raises(TypeError) as e:
        view._get_sync_request_proto()
    assert (
        str(e.value)
        == "Feature `income` is extracted by multiple extractors including `get_user_income` in featureset `UserInfo`."
    )

    sync_request = view._get_sync_request_proto(env="prod")
    assert len(sync_request.feature_sets) == 3
    assert len(sync_request.extractors) == 2
    assert len(sync_request.features) == 8

    extractor_req = sync_request.extractors[1]
    assert extractor_req.name == "get_user_info2"

    sync_request = view._get_sync_request_proto(env="dev")
    assert len(sync_request.feature_sets) == 3
    assert len(sync_request.extractors) == 2
    assert len(sync_request.features) == 8

    extractor_req = sync_request.extractors[0]
    assert extractor_req.name == "_fennel_alias_Request.user_id"

    extractor_req = sync_request.extractors[1]
    assert extractor_req.name == "_fennel_lookup_UserInfoDataset.avg_income"
