import json
import pandas as pd
from datetime import datetime
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional

import fennel.gen.featureset_pb2 as fs_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs, struct
from fennel.sources import source, Webhook

from fennel.test_lib import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    nickname: str
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


def test_valid_derived_extractors():
    @struct
    class AgeGroup:
        young: bool
        senior: bool
        group: str

    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        # alias feature
        user_id: int = feature(id=1).extract(feature=User.id)
        # lookup derived feature
        gender: str = feature(id=2).extract(
            field=UserInfoDataset.gender,
            default="unspecified",
        )
        # lookup with meta
        age_years: float = (
            feature(id=3)
            .extract(
                field=UserInfoDataset.age,
                default=0,
            )
            .meta(owner="zaki@fennel.ai")
        )
        # deprecated feature
        dob: str = (
            feature(id=4)
            .extract(
                field=UserInfoDataset.dob,
                default="unspecified",
            )
            .meta(deprecated=True)
        )
        # depends on derived feature
        age_group: AgeGroup = feature(id=5)

        # optional lookup derived feature
        optional_nickname: Optional[str] = feature(id=6).extract(
            field=UserInfoDataset.nickname,
        )

        @extractor(depends_on=[UserInfoDataset])
        @inputs(age_years)
        @outputs(age_group)
        def get_age_group(cls, ts: pd.Series, age: pd.Series):
            def age_to_group(x):
                if x < 18:
                    return AgeGroup(young=True, senior=False, group="young")
                if x > 65:
                    return AgeGroup(young=False, senior=True, group="senior")
                return AgeGroup(young=False, senior=False, group="adult")

            return age.map(lambda x: age_to_group(x))

    @meta(owner="test@test.com")
    @featureset
    class AgeInfo:
        # alias a feature that has an explicit extractor
        age_group: AgeGroup = feature(id=1).extract(feature=UserInfo.age_group)
        # alias a feature that has a derived extractor
        age: float = feature(id=2).extract(feature=UserInfo.age_years)

    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(AgeInfo)
    view.add(User)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 3
    assert len(sync_request.extractors) == 8
    assert len(sync_request.features) == 10

    # featuresets
    def test_fs(fs_request, expected_dict):
        expected_fs_request = ParseDict(
            expected_dict, fs_proto.CoreFeatureset()
        )
        # Clear the pycode field in featureset_request as it is not deterministic
        fs_request.pycode.Clear()
        assert fs_request == expected_fs_request, error_message(
            fs_request, expected_fs_request
        )

    f = {
        "name": "UserInfo",
        "metadata": {"owner": "test@test.com"},
        "pycode": {"source_code": ""},
    }
    test_fs(sync_request.feature_sets[0], f)
    f["name"] = "AgeInfo"
    test_fs(sync_request.feature_sets[1], f)

    # features
    def test_feature(actual_feature, expected_dict):
        expected_feature = ParseDict(expected_dict, fs_proto.Feature())
        assert actual_feature == expected_feature, error_message(
            actual_feature, expected_feature
        )

    age_group_struct_type = {
        "name": "AgeGroup",
        "fields": [
            {"name": "young", "dtype": {"bool_type": {}}},
            {"name": "senior", "dtype": {"bool_type": {}}},
            {"name": "group", "dtype": {"string_type": {}}},
        ],
    }

    # The comments above the feature declarations are captured as metadata
    expected_features = [
        {
            "id": 1,
            "name": "user_id",
            "dtype": {"int_type": {}},
            "metadata": {"description": "alias feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 2,
            "name": "gender",
            "dtype": {"string_type": {}},
            "metadata": {"description": "lookup derived feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 3,
            "name": "age_years",
            "dtype": {"double_type": {}},
            "metadata": {
                "owner": "zaki@fennel.ai",
                "description": "lookup with meta",
            },
            "feature_set_name": "UserInfo",
        },
        {
            "id": 4,
            "name": "dob",
            "dtype": {"string_type": {}},
            "metadata": {
                "deprecated": True,
                "description": "deprecated feature",
            },
            "feature_set_name": "UserInfo",
        },
        {
            "id": 5,
            "name": "age_group",
            "dtype": {"struct_type": age_group_struct_type},
            "metadata": {"description": "depends on derived feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 6,
            "name": "optional_nickname",
            "dtype": {"optional_type": {"of": {"string_type": {}}}},
            "metadata": {"description": "optional lookup derived feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 1,
            "name": "age_group",
            "dtype": {"struct_type": age_group_struct_type},
            "metadata": {
                "description": "alias a feature that has an explicit extractor"
            },
            "feature_set_name": "AgeInfo",
        },
        {
            "id": 2,
            "name": "age",
            "dtype": {"double_type": {}},
            "metadata": {
                "description": "alias a feature that has a derived extractor"
            },
            "feature_set_name": "AgeInfo",
        },
        {
            "id": 1,
            "name": "id",
            "dtype": {"int_type": {}},
            "metadata": {},
            "feature_set_name": "User",
        },
        {
            "id": 2,
            "name": "age",
            "dtype": {"double_type": {}},
            "metadata": {},
            "feature_set_name": "User",
        },
    ]

    for i, f in enumerate(sync_request.features):
        test_feature(f, expected_features[i])

    def test_extractor(actual, expected):
        if actual.extractor_type == fs_proto.PY_FUNC:
            actual = erase_extractor_pycode(actual)
        expected_extractor = ParseDict(expected, fs_proto.Extractor())
        assert actual == expected_extractor, error_message(
            actual, expected_extractor
        )

    expected_extractors = [
        {
            "name": "_fennel_alias_id",
            "datasets": [],
            "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
            "features": ["user_id"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.ALIAS,
            "field_info": None,
        },
        {
            "name": "_fennel_lookup_gender",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["gender"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "field_info": {
                "field": {"name": "gender", "dtype": {"string_type": {}}},
                "defaultValue": json.dumps("unspecified"),
            },
        },
        {
            "name": "_fennel_lookup_age",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["age_years"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "field_info": {
                "field": {"name": "age", "dtype": {"int_type": {}}},
                "defaultValue": json.dumps(0),
            },
        },
        {
            "name": "_fennel_lookup_dob",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["dob"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "field_info": {
                "field": {"name": "dob", "dtype": {"string_type": {}}},
                "defaultValue": json.dumps("unspecified"),
            },
        },
        {
            "name": "_fennel_lookup_nickname",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["optional_nickname"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "field_info": {
                "field": {"name": "nickname", "dtype": {"string_type": {}}},
                "defaultValue": json.dumps(None),
            },
        },
        {
            "name": "get_age_group",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {
                    "feature": {
                        "feature_set_name": "UserInfo",
                        "name": "age_years",
                    }
                }
            ],
            "features": ["age_group"],
            "metadata": {},
            "version": 0,
            "pycode": {"source_code": ""},
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.PY_FUNC,
            "field_info": None,
        },
        {
            "name": "_fennel_alias_age_group",
            "datasets": [],
            "inputs": [
                {
                    "feature": {
                        "feature_set_name": "UserInfo",
                        "name": "age_group",
                    }
                }
            ],
            "features": ["age_group"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "AgeInfo",
            "extractor_type": fs_proto.ALIAS,
            "field_info": None,
        },
        {
            "name": "_fennel_alias_age_years",
            "datasets": [],
            "inputs": [
                {
                    "feature": {
                        "feature_set_name": "UserInfo",
                        "name": "age_years",
                    }
                }
            ],
            "features": ["age"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "AgeInfo",
            "extractor_type": fs_proto.ALIAS,
            "field_info": None,
        },
    ]
    for i, e in enumerate(sync_request.extractors):
        test_extractor(e, expected_extractors[i])
